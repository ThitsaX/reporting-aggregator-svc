import mongoose from 'mongoose';
import { initializeMongoClient } from '../infra';
import { TransactionModel } from '../schemas';
import { logger } from '../utils';


async function createTransactionIndex() {
  const indexName = 'idx_lastUpdated';

  try {
    await initializeMongoClient();
    logger.info('Transaction index creation started');
    const startTimeMs = Date.now();

    const result = await TransactionModel.collection.createIndex(
      { lastUpdated: -1 },
      { name: indexName },
    );

    const endTimeMs = Date.now();
    logger.info(`Created MongoDB transaction index: ${result}`);

    const timeTakenMs = endTimeMs - startTimeMs;
    const timeTakenMint = (timeTakenMs / 1000) / 60;
    const timeTakenHour = timeTakenMint / 60;
    logger.info(`Time taken in milliseconds: ${timeTakenMs}`);
    logger.info(`Time taken in minutes: ${timeTakenMint}`);
    logger.info(`Time taken in hours: ${timeTakenHour}`);

  } catch(error) {
    logger.error('Failed to create MongoDB transaction lastUpdated index', error);
    process.exitCode = 1;
  } finally {
    await mongoose.disconnect();
    logger.info('Connection to mongodb ended');
  }
}

createTransactionIndex();
