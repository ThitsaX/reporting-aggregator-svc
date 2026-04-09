import config from '../config';
import knex, { Knex } from 'knex';
import { logger } from '../utils';

const SLOW_QUERY_LOG_MS = 100;
const MAX_SQL_LOG_LENGTH = 240;

function summarizeSql(sql: string | undefined): string {
  if (!sql) return '';
  const normalized = sql.replace(/\s+/g, ' ').trim();
  return normalized.length > MAX_SQL_LOG_LENGTH
    ? `${normalized.slice(0, MAX_SQL_LOG_LENGTH)}...`
    : normalized;
}

export function initializeMySQLClient(): Knex {
  const knexClient = knex({
    client: 'mysql2',
    connection: {
      host: config.get('REPORTING_MYSQL_DB.HOST'),
      port: config.get('REPORTING_MYSQL_DB.PORT'),
      user: config.get('REPORTING_MYSQL_DB.USER'),
      password: config.get('REPORTING_MYSQL_DB.PASSWORD'),
      database: config.get('REPORTING_MYSQL_DB.SCHEMA'),
      timezone: 'Z',  // To take the time from mysql as it is and not convert it to local timezone
      ...config.get('REPORTING_MYSQL_DB.ADDITIONAL_CONNECTION_OPTIONS'),
    },
    pool: { min: 10, max: 10 }, // Default pool size, as no DB_CONNECTION_LIMIT provided
  });

  const queryStartTimes = new Map<string, number>();

  knexClient.on('query', (queryData) => {
    if (!queryData.__knexQueryUid) return;
    queryStartTimes.set(queryData.__knexQueryUid, Date.now());
  });

  knexClient.on('query-response', (response, queryData) => {
    if (!queryData.__knexQueryUid) return;

    const startTime = queryStartTimes.get(queryData.__knexQueryUid);
    queryStartTimes.delete(queryData.__knexQueryUid);
    if (!startTime) return;

    const durationMs = Date.now() - startTime;
    if (durationMs < SLOW_QUERY_LOG_MS) return;

    const rowCount = Array.isArray(response) ? response.length : undefined;
    logger.info('Slow MySQL query', {
      durationMs,
      rowCount,
      sql: summarizeSql(queryData.sql),
      bindingsCount: queryData.bindings?.length ?? 0,
    });
  });

  knexClient.on('query-error', (error, queryData) => {
    if (!queryData.__knexQueryUid) return;

    const startTime = queryStartTimes.get(queryData.__knexQueryUid);
    queryStartTimes.delete(queryData.__knexQueryUid);

    logger.error('MySQL query failed', {
      durationMs: startTime ? Date.now() - startTime : undefined,
      sql: summarizeSql(queryData.sql),
      bindingsCount: queryData.bindings?.length ?? 0,
      error,
    });
  });

  return knexClient;
}
