import convict from 'convict';
import { AppConfig } from './infra';

const config = convict<AppConfig>({
  LOG_LEVEL: {
    doc: 'Logger level',
    format: String, // todo: use LogLevel type
    default: 'info',
    env: 'LOG_LEVEL',
  },
  REPORTING_MYSQL_DB: {
    HOST: {
      doc: 'The Hostname/IP address of database',
      format: '*',
      default: 'localhost',
      env: 'REPORTING_MYSQL_DB_HOST',
    },
    PORT: {
      doc: 'The port of database',
      format: 'port',
      default: 3306,
      env: 'REPORTING_MYSQL_DB_PORT',
    },
    USER: {
      doc: 'The username for database',
      format: '*',
      default: 'central_ledger',
      env: 'REPORTING_MYSQL_DB_USER',
    },
    PASSWORD: {
      doc: 'The password for database',
      format: '*',
      default: 'password',
      env: 'REPORTING_MYSQL_DB_PASSWORD',
      sensitive: true,
    },
    SCHEMA: {
      doc: 'The schema in database',
      format: '*',
      default: 'central_ledger',
      env: 'REPORTING_MYSQL_DB_SCHEMA',
    },
    ADDITIONAL_CONNECTION_OPTIONS: {
      doc: 'Additional options to pass to the database connection',
      format: Object,
      default: {},
      env: 'REPORTING_MYSQL_DB_ADDITIONAL_CONNECTION_OPTIONS',
    },
    QUERY_TIMEOUT_MS: {
      doc: 'Query timeout for each polling',
      format: Number,
      default: 30000,
      env: 'QUERY_TIMEOUT_MS',
    },
  },
  REPORTING_MONGO_DB: {
    HOST: {
      doc: 'The Hostname/IP address of database',
      format: '*',
      default: 'localhost',
      env: 'REPORTING_MONGO_DB_HOST',
    },
    PORT: {
      doc: 'The port number of database',
      format: 'port',
      default: 27017,
      env: 'REPORTING_MONGO_DB_PORT',
    },
    USER: {
      doc: 'The user of database',
      format: '*',
      default: 'test',
      env: 'REPORTING_MONGO_DB_USER',
    },
    PASSWORD: {
      doc: 'The password of database',
      format: '*',
      default: 'test123',
      env: 'REPORTING_MONGO_DB_PASSWORD',
      sensitive: true,
    },
    DATABASE: {
      doc: 'The database name in database',
      format: '*',
      default: 'admin',
      env: 'REPORTING_MONGO_DB_DATABASE',
    },
    BULK_WRITE_TIMEOUT_MS: {
      doc: 'Operation timeout for killing slow bulk writes',
      format: Number,
      default: 30000,
      env: 'BULK_WRITE_TIMEOUT_MS',
    },
    BULK_WRITE_SIZE: {
      doc: 'Total records in a smaller sub batch to bulk write to mongodb',
      format: Number,
      default: 1000,
      env: 'BULK_WRITE_SIZE',
    },
    PARAMS: {
      doc: 'Additional parameters for MongoDB connection',
      format: function (val) {
        if (typeof val === 'string') {
          try {
            JSON.parse(val);
            return val;
          } catch (e) {
            throw new Error(`REPORTING_MONGO_DB_PARAMS must be valid JSON: ${e}`);
          }
        } else if (typeof val !== 'object') {
          throw new Error('REPORTING_MONGO_DB_PARAMS must be an object or a JSON string');
        }
        return val;
      },
      default: {},
      env: 'REPORTING_MONGO_DB_PARAMS',
    },
  },
  PAUSE_TRANSFER_AGGREGATOR: {
    doc: 'Option to pause transfer_aggregator for debugging purposes',
    format: Boolean,
    default: false,
    env: 'PAUSE_TRANSFER_AGGREGATOR',
  },
  PAUSE_SETTLEMENT_AGGREGATOR: {
    doc: 'Option to pause settlement_aggregator for debugging purposes',
    format: Boolean,
    default: false,
    env: 'PAUSE_SETTLEMENT_AGGREGATOR',
  },
  PAUSE_FXTRANSFER_AGGREGATOR: {
    doc: 'Option to pause fxtransfer_aggregator for debugging purposes',
    format: Boolean,
    default: false,
    env: 'PAUSE_FXTRANSFER_AGGREGATOR',
  },
  BATCH_SIZE: {
    doc: 'Number of transferStateChangeIds to process per batch',
    format: Number,
    default: 10000,
    env: 'BATCH_SIZE',
  },
  TRANSFER_DETAILS_BATCH_SIZE: {
    doc: 'Number of transferIds to fetch per transfer detail query chunk',
    format: Number,
    default: 1000,
    env: 'TRANSFER_DETAILS_BATCH_SIZE',
  },
  LOOP_TIMEOUT: {
    doc: 'Loop timeout (in milliseconds) before the next states are fetched.',
    format: Number,
    default: 5000,
    env: 'LOOP_TIMEOUT',
  },
  MIN_BATCH_PERCENTAGE: {
    doc: 'Minimum percentage of batch required to be processed before polling again',
    format: Number,
    default: 70,
    env: 'MIN_BATCH_PERCENTAGE',
  },
  WAIT_TIMEOUT_MS: {
    doc: 'Timeout before fetching missing id(s)',
    format: Number,
    default: 5000,
    env: 'WAIT_TIMEOUT_MS',
  },
  MAX_WAIT_COUNT: {
    doc: 'Maximum number of times to wait before skipping missing id(s)',
    format: Number,
    default: 3,
    env: 'MAX_WAIT_COUNT',
  },
});

if (process.env['REPORTING_MYSQL_DB_SSL_ENABLED'] === 'true') {
  config.set('REPORTING_MYSQL_DB.ADDITIONAL_CONNECTION_OPTIONS.ssl', {
    rejectUnauthorized: process.env['REPORTING_MYSQL_DB_SSL_VERIFY'] === 'true',
  });
  // Add CA certificate if environment variable is set
  const sslCa = process.env['REPORTING_MYSQL_DB_SSL_CA'];
  if (sslCa) {
    config.set('REPORTING_MYSQL_DB.ADDITIONAL_CONNECTION_OPTIONS.ssl.ca', sslCa);
  }
}

config.validate({ allowed: 'strict' });

if (process.env['REPORTING_MONGO_DB_SSL_ENABLED'] === 'true') {
  config.set('REPORTING_MONGO_DB.SSL_ENABLED', true);
  config.set('REPORTING_MONGO_DB.SSL_VERIFY', process.env['REPORTING_MONGO_DB_SSL_VERIFY'] === 'true');
  // Add CA certificate if environment variable is set
  const sslCa = process.env['REPORTING_MONGO_DB_SSL_CA_FILE_PATH'];
  if (sslCa) {
    config.set('REPORTING_MONGO_DB.SSL_CA_FILE_PATH', sslCa);
  }
} else {
  config.set('REPORTING_MONGO_DB.SSL_ENABLED', false);
}

export default config;
