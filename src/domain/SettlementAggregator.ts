import { IAggregator, IAggDeps } from '../types';
import { ISettlement } from '../schemas';

interface SettlementStateChange {
  settlementId: number;
  settlementStateId: string;
  settlementStateChangeId: number;
}

interface SettlementDetail {
  settlementId: bigint;
  settlementStateChangeId: number;
  createdAt: Date;
  lastUpdated: Date;
  settlementStatus: string;
  settlementModel: string;
  reason?: string;
}

interface SettlementWindow {
  settlementId: bigint;
  settlementWindowId: bigint;
}

export class SettlementAggregator implements IAggregator {
  private isRunning: boolean = false;
  private processName: string;

  constructor(
    private readonly deps: IAggDeps,
    processName: string = 'settlement_process',
  ) {
    this.processName = processName;
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      this.deps.logger.info('Settlement Aggregator is already running');
      return;
    }

    // Check if the aggregator is paused
    if (this.deps.pauseSettlementAggregator) {
      this.deps.logger.info('Settlement Aggregator is paused. To resume, change env PAUSE_SETTLEMENT_AGGREGATOR to false and restart the service');
      return;
    }

    this.isRunning = true;
    this.deps.logger.info('Settlement Aggregator is started');

    try {
      await this.processSettlements();
    } catch (error) {
      this.deps.logger.error('Settlement Aggregator failed', error);
      throw error;
    }
  }

  async stop(): Promise<void> {
    if (!this.isRunning) {
      this.deps.logger.info('Settlement Aggregator is already stopped');
      return;
    }

    this.isRunning = false;
    this.deps.logger.info('Settlement Aggregator is stopped');
  }

  getDeps(): IAggDeps {
    return this.deps;
  }

  private async processRecord(detail: SettlementDetail, windowIds: bigint[]): Promise<ISettlement | null> {
    if (!detail.settlementId) return null;

    return {
      settlementId: detail.settlementId,
      createdAt: detail.createdAt,
      lastUpdatedAt: detail.lastUpdated,
      settlementStatus: detail.settlementStatus.trim(),
      settlementModel: detail.settlementModel.trim(),
      settlementWindows: windowIds.map((id) => ({ settlementWindowId: id })),
      settlementStateChange: [
        {
          reason: detail.reason?.trim(),
          status: detail.settlementStatus?.trim(),
          dateTime: detail.lastUpdated,
        },
      ],
    };
  }

  private async processSettlements(): Promise<void> {
    let lastId = await this.deps.stateModel
      .findOne({ process: this.processName })
      .then((doc) => (doc ? doc.lastId : 0));

    while (this.isRunning) {
      try {
        const settlementStateChanges: SettlementStateChange[] = await this.deps
          .knexClient('settlementStateChange')
          .select('settlementId', 'settlementStateId', 'settlementStateChangeId')
          .where('settlementStateChangeId', '>', lastId)
          .orderBy('settlementStateChangeId')
          .limit(this.deps.batchSize);

        if (!settlementStateChanges.length) {
          await new Promise((resolve) => setTimeout(resolve, this.deps.timeout));
          continue;
        }

        // @ts-expect-error { Object is possibly "undefined"}
        let newLastId = settlementStateChanges[settlementStateChanges.length - 1].settlementStateChangeId;
        const mongoBatch = [];
        const settlementStateChangeIds = settlementStateChanges.map((row) => row.settlementStateChangeId);
        const settlementIds = [...new Set(settlementStateChanges.map((row) => row.settlementId))];
        const settlementStateChangePlaceholders = settlementStateChangeIds.map(() => '?').join(',');
        const settlementPlaceholders = settlementIds.map(() => '?').join(',');

        const windowQuery = `
          SELECT 
            s.settlementId as settlementId,
            ssw.settlementWindowId 
          FROM settlement s
          JOIN settlementSettlementWindow ssw ON ssw.settlementId = s.settlementId
          WHERE s.settlementId IN (${settlementPlaceholders})
        `;

        const detailsQuery = `
          SELECT 
            s.settlementId as settlementId,
            ssc.settlementStateChangeId as settlementStateChangeId,
            s.createdDate as createdAt,
            ssc.createdDate as lastUpdated,
            ssc.settlementStateId as settlementStatus,
            sm.name as settlementModel,
            ssc.reason as reason
          FROM settlementStateChange ssc
          JOIN settlement s ON s.settlementId = ssc.settlementId
          JOIN settlementModel sm ON sm.settlementModelId = s.settlementModelId
          WHERE ssc.settlementStateChangeId IN (${settlementStateChangePlaceholders})
        `;

        const detailsResult = await this.deps.knexClient.raw(detailsQuery, settlementStateChangeIds);
        const details = detailsResult[0] as SettlementDetail[];
        const detailByStateChangeId = new Map(details.map((detail) => [detail.settlementStateChangeId, detail]));

        const windowsResult = await this.deps.knexClient.raw(windowQuery, settlementIds);
        const windows = windowsResult[0] as SettlementWindow[];
        const windowIdsBySettlementId = new Map<number, bigint[]>();
        for (const window of windows) {
          const key = Number(window.settlementId);
          const existing = windowIdsBySettlementId.get(key) ?? [];
          existing.push(window.settlementWindowId);
          windowIdsBySettlementId.set(key, existing);
        }

        for (const row of settlementStateChanges) {
          const { settlementId, settlementStateChangeId } = row;
          const detail = detailByStateChangeId.get(settlementStateChangeId);
          if (!detail) continue;

          newLastId = settlementStateChangeId;
          const windowIds = windowIdsBySettlementId.get(settlementId) ?? [];
          const processedData = await this.processRecord(detail, windowIds);

          if (processedData) {
            mongoBatch.push({
              updateOne: {
                filter: { settlementId: processedData.settlementId },
                update: { $set: processedData },
                upsert: true,
              },
            });
          }
        }

        if (mongoBatch.length > 0) {
          await this.deps.settlementModel.bulkWrite(mongoBatch);
        }

        await this.deps.stateModel.updateOne(
          { process: this.processName },
          { $set: { lastId: newLastId, updatedAt: new Date() } },
          { upsert: true },
        );
        lastId = newLastId;
        this.deps.logger.info(`Processed up to settlementStateChangeId ${lastId}`);
      } catch (error) {
        this.deps.logger.error(`Error in ${this.processName}`, error);
      } finally {
        await new Promise((resolve) => setTimeout(resolve, this.deps.timeout));
      }
    }
  }
}
