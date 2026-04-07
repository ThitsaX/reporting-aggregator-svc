/* eslint-disable-next-line @typescript-eslint/no-unused-vars */
import { HEALTH_STATUSES } from '../constants';

export type AppConfig = {
  LOG_LEVEL: string;
  REPORTING_MYSQL_DB: {
    HOST: string;
    PORT: number;
    USER: string;
    PASSWORD: string;
    SCHEMA: string;
    QUERY_TIMEOUT_MS: number;
    ADDITIONAL_CONNECTION_OPTIONS?: Record<string, unknown>
  };
  REPORTING_MONGO_DB: {
    HOST: string;
    PORT: number;
    USER: string;
    PASSWORD: string;
    DATABASE: string;
    BULK_WRITE_TIMEOUT_MS: number;
    BULK_WRITE_SIZE: number;
    PARAMS: Record<string, unknown>; // Additional parameters for MongoDB connection
    SSL_ENABLED?: boolean; // Optional, defaults to false
    SSL_VERIFY?: boolean; // Optional, defaults to true
    SSL_CA_FILE_PATH?: string; // Optional, CA certificate file path
  };
  BATCH_SIZE: number;
  TRANSFER_DETAILS_BATCH_SIZE: number;
  LOOP_TIMEOUT: number;
  MIN_BATCH_PERCENTAGE: number;
  MAX_WAIT_COUNT: number;
};

type Status = (typeof HEALTH_STATUSES)[keyof typeof HEALTH_STATUSES];

export type HealthcheckDetails = {
  isReady: boolean;
};

export type HealthcheckState = {
  status: Status;
  details: HealthcheckDetails; // or rename to state?
  startTime: string; // ISO date string
  versionNumber: string;
};
