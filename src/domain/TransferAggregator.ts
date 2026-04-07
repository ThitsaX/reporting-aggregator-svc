import { ITransaction } from '#src/schemas';
import { IAggregator, IAggDeps, TransferStateChange, Record, KnexRawResult } from '../types';

type PositionChangeRecord = {
  transferId: string;
  participantName?: string;
  currency?: string;
  ledgerType?: string;
  dateTime?: Date;
  updatedPosition?: number;
  positionChange?: number;
};

export class TransferAggregator implements IAggregator {
  private isRunning: boolean = false;
  private processName: string;

  constructor(
    private readonly deps: IAggDeps,
    processName: string = 'transactions_process',
  ) {
    this.processName = processName;
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      this.deps.logger.info('Transfer Aggregator is already running');
      return;
    }

    this.isRunning = true;
    this.deps.logger.info('Transfer Aggregator is started');

    try {
      await this.processTransactions();
    } catch (error) {
      this.deps.logger.error('Transfer Aggregator failed', error);
      throw error;
    }
  }

  async stop(): Promise<void> {
    if (!this.isRunning) {
      this.deps.logger.info('Transfer Aggregator is already stopped');
      return;
    }

    this.isRunning = false;
    // await this.deps.knexClient.destroy();
    this.deps.logger.info('Transfer Aggregator is stopped');
  }

  getDeps(): IAggDeps {
    return this.deps;
  }

  private chunkTransferIds(transferIds: string[]): string[][] {
    const chunkSize = Math.min(this.deps.batchSize, this.deps.transferDetailsBatchSize);
    const chunks: string[][] = [];

    for (let index = 0; index < transferIds.length; index += chunkSize) {
      chunks.push(transferIds.slice(index, index + chunkSize));
    }

    return chunks;
  }

  private mergeUnique<T>(existing: T[], incoming: T[], keyFn: (item: T) => string): T[] {
    const merged = [...existing];
    const seen = new Set(existing.map((item) => keyFn(item)));

    for (const item of incoming) {
      const key = keyFn(item);
      if (!seen.has(key)) {
        merged.push(item);
        seen.add(key);
      }
    }

    return merged;
  }

  private mergeTransaction(existing: ITransaction, incoming: ITransaction): ITransaction {
    return {
      ...existing,
      ...incoming,
      transferStateChanges: this.mergeUnique(
        existing.transferStateChanges,
        incoming.transferStateChanges,
        (stateChange) => [
          stateChange.transferState,
          stateChange.transferStateEnum,
          stateChange.reason ?? '',
          stateChange.dateTime ? new Date(stateChange.dateTime).toISOString() : '',
        ].join('|'),
      ),
      positionChanges: this.mergeUnique(
        existing.positionChanges,
        incoming.positionChanges,
        (positionChange) => [
          positionChange.participantName ?? '',
          positionChange.currency ?? '',
          positionChange.ledgerType ?? '',
          positionChange.dateTime ? new Date(positionChange.dateTime).toISOString() : '',
          positionChange.updatedPosition ?? '',
          positionChange.change ?? '',
        ].join('|'),
      ),
    };
  }

  private async fetchTransferDetails(transferIds: string[]): Promise<Record[]> {
    const transferStateChangeIds = transferIds
      .map((transferId) => this.transferStateChangeIdByTransferId.get(transferId))
      .filter((transferStateChangeId): transferStateChangeId is number => transferStateChangeId != null);

    if (!transferIds.length || !transferStateChangeIds.length) {
      return [];
    }

    const transferPlaceholders = Array(transferIds.length).fill('?').join(',');
    const stateChangePlaceholders = Array(transferStateChangeIds.length).fill('?').join(',');
    const rawResult = (await this.deps.knexClient.raw(
      `
      SELECT 
        transfer.transferId,
        q.transactionReferenceId as transactionId,
        ft.sourceAmount,
        ft.sourceCurrency,
        ft.targetAmount,
        ft.targetCurrency,
        transfer.createdDate as createdAt,
        transfer.amount,
        transfer.currencyId AS currency,
        tsc.transferStateChangeId AS transferStateChangeId,
        tsc.transferStateId AS transferStateChangeState,
        tsc.reason AS transferStateChangeReason,
        tsc.createdDate AS transferStateChangeDateTime,
        ts.enumeration AS transferStateEnum,
        ts2.name as transactionType,
        tss.name AS transactionSubScenario,
        ti.name AS transactionInitiator,
        tit.name AS transactionInitiatorType,
        te.errorCode,
        te.errorDescription,
        CASE WHEN da.isProxy = 0 THEN da.name ELSE ep1.name END as payerDFSP,
        CASE WHEN da.isProxy = 1 THEN da.name ELSE NULL END as payerDFSPProxy,
        da.description AS payerDesc,
        CASE WHEN ca.isProxy = 0 THEN ca.name ELSE ep2.name END as payeeDFSP,
        CASE WHEN ca.isProxy = 1 THEN ca.name ELSE NULL END as payeeDFSPProxy,
        ca.description AS payeeDesc,
        payerpit.name as payerPartyIdType,
        qp1.partyIdentifierValue as payerPartyIdentifier,
        qp1.partyName as payerPartyName,
        payeepit.name as payeePartyIdType,
        qp2.partyIdentifierValue as payeePartyIdentifier,
        qp2.partyName as payeePartyName,
        qr.quoteId,
        at2.name as quoteRequestAmountType,
        q.currencyId as quoteRequestCurrency,
        q.amount as quoteRequestAmount,
        qr.transferAmount as transferTermsTransferAmount,
        qr.transferAmountCurrencyId as transferTermsTransferCurrency,
        qr.payeeReceiveAmountCurrencyId as transferTermsPayeeReceiveCurrency,
        qr.payeeReceiveAmount as transferTermsPayeeReceiveAmount,
        qr.payeeFspFeeAmount as transferTermsPayeeFspFeeAmount,
        qr.payeeFspFeeCurrencyId as transferTermsPayeeFspFeeCurrency,
        qr.payeeFspCommissionCurrencyId as transferTermsPayeeFspCommissionCurrency,
        qr.payeeFspCommissionAmount as transferTermsPayeeFspCommissionAmount,
        qr.responseExpirationDate as transferTermsExpiration,
        ilpp.value as ilpPacket,
        tf.settlementWindowId as transferSettlementWindowId,
        gc.latitude as geoCodeLatitude,
        gc.longitude as geoCodeLongitude,
        tss.name AS baseUseCase 
      FROM transfer
        INNER JOIN transferParticipant AS tp1 ON tp1.transferId = transfer.transferId
        LEFT JOIN externalParticipant AS ep1 ON ep1.externalParticipantId = tp1.externalParticipantId
        INNER JOIN transferParticipantRoleType AS tprt1 ON tprt1.transferParticipantRoleTypeId = tp1.transferParticipantRoleTypeId
        INNER JOIN participant AS da ON da.participantId = tp1.participantId
        LEFT JOIN participantCurrency AS pc1 ON pc1.participantCurrencyId = tp1.participantCurrencyId
        INNER JOIN transferParticipant AS tp2 ON tp2.transferId = transfer.transferId
        LEFT JOIN externalParticipant AS ep2 ON ep2.externalParticipantId = tp2.externalParticipantId
        INNER JOIN transferParticipantRoleType AS tprt2 ON tprt2.transferParticipantRoleTypeId = tp2.transferParticipantRoleTypeId
        INNER JOIN participant AS ca ON ca.participantId = tp2.participantId
        LEFT JOIN participantCurrency AS pc2 ON pc2.participantCurrencyId = tp2.participantCurrencyId
        INNER JOIN ilpPacket AS ilpp ON ilpp.transferId = transfer.transferId
        LEFT JOIN transferStateChange AS tsc ON tsc.transferId = transfer.transferId
          AND tsc.transferStateChangeId IN (${stateChangePlaceholders})
        LEFT JOIN transferState AS ts ON ts.transferStateId = tsc.transferStateId
        LEFT JOIN transferFulfilment AS tf ON tf.transferId = transfer.transferId
        LEFT JOIN transferError AS te ON te.transferId = transfer.transferId
        LEFT JOIN fxTransfer AS ft ON ft.determiningTransferId = transfer.transferId
        INNER JOIN quote AS q ON q.transactionReferenceId = transfer.transferId
        INNER JOIN transactionScenario AS ts2 ON ts2.transactionScenarioId = q.transactionScenarioId
        INNER JOIN transactionSubScenario AS tss ON tss.transactionSubScenarioId = q.transactionSubScenarioId
        INNER JOIN transactionInitiator AS ti ON ti.transactionInitiatorId = q.transactionInitiatorId
        INNER JOIN transactionInitiatorType AS tit ON tit.transactionInitiatorTypeId = q.transactionInitiatorTypeId
        INNER JOIN quoteParty AS qp1 ON q.quoteId = qp1.quoteId AND qp1.partyTypeId = tprt1.transferParticipantRoleTypeId
        INNER JOIN quoteParty AS qp2 ON q.quoteId = qp2.quoteId AND qp2.partyTypeId = tprt2.transferParticipantRoleTypeId
        INNER JOIN partyIdentifierType AS payerpit ON payerpit.partyIdentifierTypeId = qp1.partyIdentifierTypeId
        INNER JOIN partyIdentifierType AS payeepit ON payeepit.partyIdentifierTypeId = qp2.partyIdentifierTypeId
        INNER JOIN quoteResponse AS qr ON qr.quoteId = q.quoteId
        INNER JOIN amountType at2 ON q.amountTypeId = at2.amountTypeId
        LEFT JOIN geoCode gc ON gc.quotePartyId = qp2.quotePartyId
      WHERE transfer.transferId IN (${transferPlaceholders})
        AND tprt1.name = 'PAYER_DFSP'
        AND tprt2.name = 'PAYEE_DFSP'
      ORDER BY tsc.transferStateChangeId
      `,
      [...transferStateChangeIds, ...transferIds],
    )) as KnexRawResult;

    return rawResult[0];
  }

  private transferStateChangeIdByTransferId = new Map<string, number>();

  private async fetchPositionChanges(transferIds: string[]): Promise<PositionChangeRecord[]> {
    const transferStateChangeIds = transferIds
      .map((transferId) => this.transferStateChangeIdByTransferId.get(transferId))
      .filter((transferStateChangeId): transferStateChangeId is number => transferStateChangeId != null);

    if (!transferStateChangeIds.length) {
      return [];
    }

    const stateChangePlaceholders = Array(transferStateChangeIds.length).fill('?').join(',');
    const rawResult = (await this.deps.knexClient.raw(
      `
      SELECT
        tsc.transferId,
        pa.name AS participantName,
        pc3.currencyId AS currency,
        lat.name AS ledgerType,
        ppc.createdDate AS dateTime,
        ppc.value AS updatedPosition,
        ppc.\`change\` AS positionChange
      FROM participantPositionChange ppc
        INNER JOIN transferStateChange tsc ON tsc.transferStateChangeId = ppc.transferStateChangeId
        LEFT JOIN participantCurrency pc3 ON pc3.participantCurrencyId = ppc.participantCurrencyId
        LEFT JOIN participant pa ON pa.participantId = pc3.participantId
        LEFT JOIN ledgerAccountType lat ON lat.ledgerAccountTypeId = pc3.ledgerAccountTypeId
      WHERE ppc.transferStateChangeId IN (${stateChangePlaceholders})
      ORDER BY ppc.transferStateChangeId, ppc.participantPositionChangeId
      `,
      transferStateChangeIds,
    )) as [PositionChangeRecord[], unknown];

    return rawResult[0];
  }

  private async processRecord(record: Record): Promise<ITransaction | null> {
    if (!record.amount || record.amount <= 0) return null;

    const stateChange = {
      dateTime: record.transferStateChangeDateTime,
      reason: record.transferStateChangeReason,
      transferState: record.transferStateChangeState,
      transferStateEnum: record.transferStateEnum,
    };

    return {
      transferId: record.transferId,
      transactionId: record.transactionId,
      sourceAmount: record.sourceAmount ? record.sourceAmount : record.amount,
      sourceCurrency: record.sourceCurrency ? record.sourceCurrency : record.currency,
      targetAmount: record.targetAmount ? record.targetAmount : record.amount,
      targetCurrency: record.targetCurrency ? record.targetCurrency : record.currency,
      createdAt: new Date(record.createdAt.toISOString()),
      baseUseCase: record.baseUseCase,
      lastUpdated: record.transferStateChangeDateTime,
      transferState: stateChange ? stateChange.transferState : '',
      transferStateEnum: stateChange ? stateChange.transferStateEnum : '',
      transferStateChanges: [stateChange],
      transactionType: record.transactionType,
      transactionTypeDetail: {
        scenario: record.transactionType,
        subScenario: record.transactionSubScenario,
        initiator: record.transactionInitiator,
        initiatorType: record.transactionInitiatorType,
      },
      errorCode: record.errorCode,
      errorDescription: record.errorDescription,
      transferSettlementWindowId: record.transferSettlementWindowId,
      payerDFSP: record.payerDFSP,
      payerDFSPProxy: record.payerDFSPProxy,
      payerDesc: record.payerDesc,
      payeeDFSP: record.payeeDFSP,
      payeeDFSPProxy: record.payeeDFSPProxy,
      payeeDesc: record.payeeDesc,
      positionChanges: record.positionChangesParticipantName
        ? [
          {
            participantName: record.positionChangesParticipantName,
            currency: record.positionChangesCurrency,
            ledgerType: record.positionChangesLedgerType,
            dateTime: record.positionChangesDateTime,
            updatedPosition: record.positionChangesUpdatedValue,
            change: record.positionChangesChange,
          },
        ]
        : [],
      payerParty: {
        partyIdType: record.payerPartyIdType,
        partyIdentifier: record.payerPartyIdentifier,
        partyName: record.payerPartyName,
        supportedCurrencies: '', // TODO: Map supportedCurrencies in the future
      },
      payeeParty: {
        partyIdType: record.payeePartyIdType,
        partyIdentifier: record.payeePartyIdentifier,
        partyName: record.payeePartyName,
        supportedCurrencies: '', // TODO: Map supportedCurrencies in the future
      },
      quoteRequest: {
        quoteId: record.quoteId,
        amountType: record.quoteRequestAmountType,
        amount: {
          currency: record.quoteRequestCurrency,
          amount: record.quoteRequestAmount ? record.quoteRequestAmount : 0,
        },
        fees: {
          currency: '', // No mapping available at the moment
          amount: 0, // No mapping available at the moment
        },
      },
      transferTerms: {
        transferAmount: {
          currency: record.transferTermsTransferCurrency,
          amount: record.transferTermsTransferAmount,
        },
        payeeReceiveAmount: {
          currency: record.transferTermsPayeeReceiveCurrency,
          amount: record.transferTermsPayeeReceiveAmount ? record.transferTermsPayeeReceiveAmount : 0,
        },
        payeeFspFee: {
          currency: record.transferTermsPayeeFspFeeCurrency,
          amount: record.transferTermsPayeeFspFeeAmount ? record.transferTermsPayeeFspFeeAmount : 0,
        },
        payeeFspCommission: {
          currency: record.transferTermsPayeeFspCommissionCurrency,
          amount: record.transferTermsPayeeFspCommissionAmount ? record.transferTermsPayeeFspCommissionAmount : 0,
        },
        expiration: record.transferTermsExpiration,
        geoCode: {
          latitude: record.geoCodeLatitude != null ? String(record.geoCodeLatitude) : '',
          longitude: record.geoCodeLongitude != null ? String(record.geoCodeLongitude) : '',
        },
        ilpPacket: record.ilpPacket,
      },
    };
  }

  private async processTransactions(): Promise<void> {
    let waitCount = 0;
    let lastId = await this.deps.stateModel
      .findOne({ process: this.processName })
      .then((doc) => (doc ? doc.lastId : 0));

    while (this.isRunning) {
      try {
        const transferStateChanges: TransferStateChange[] = await this.deps
          .knexClient('transferStateChange')
          .select('transferId', 'transferStateChangeId')
          .where('transferStateChangeId', '>', lastId)
          .orderBy('transferStateChangeId')
          .limit(this.deps.batchSize);

        if (!transferStateChanges.length) {
          await new Promise((resolve) => setTimeout(resolve, this.deps.timeout));
          continue;
        }

        let batchTransferIds: string[] = [];
        let newLastId = lastId;
        const selectedTransferStateChanges: TransferStateChange[] = [];

        for (const transferStateChange of transferStateChanges) {
          const currentStateId = transferStateChange.transferStateChangeId;
          if ((currentStateId - newLastId) > 1) {
            this.deps.logger.info(`Gap detected between ${newLastId} and ${currentStateId}`);
            if (waitCount < this.deps.maxWaitCount) break;
          }

          batchTransferIds.push(transferStateChange.transferId);
          selectedTransferStateChanges.push(transferStateChange);
          newLastId = currentStateId;
        }

        const currentBatchPercentage = (batchTransferIds.length * 100) / transferStateChanges.length;
        if (currentBatchPercentage < this.deps.minBatchPercentage) {
          await new Promise((resolve) => setTimeout(resolve, this.deps.timeout));
          this.deps.logger.info(`Waited for ${this.deps.timeout}ms at id ${newLastId}`);
          waitCount++;
          continue;
        }

        batchTransferIds = [...new Set(batchTransferIds)];
        this.transferStateChangeIdByTransferId = new Map(
          selectedTransferStateChanges.map((row) => [row.transferId, row.transferStateChangeId]),
        );
        const transactionsByTransferId = new Map<string, ITransaction>();

        for (const transferIdChunk of this.chunkTransferIds(batchTransferIds)) {
          const records = await this.fetchTransferDetails(transferIdChunk);
          for (const record of records) {
            const processedData = await this.processRecord(record);
            if (!processedData) continue;

            const existingTransaction = transactionsByTransferId.get(processedData.transferId);
            transactionsByTransferId.set(
              processedData.transferId,
              existingTransaction ? this.mergeTransaction(existingTransaction, processedData) : processedData,
            );
          }

          const positionChanges = await this.fetchPositionChanges(transferIdChunk);
          for (const positionChange of positionChanges) {
            const transaction = transactionsByTransferId.get(positionChange.transferId);
            if (!transaction) continue;

            transaction.positionChanges = this.mergeUnique(
              transaction.positionChanges,
              [{
                participantName: positionChange.participantName,
                currency: positionChange.currency,
                ledgerType: positionChange.ledgerType,
                dateTime: positionChange.dateTime,
                updatedPosition: positionChange.updatedPosition,
                change: positionChange.positionChange,
              }],
              (change) => [
                change.participantName ?? '',
                change.currency ?? '',
                change.ledgerType ?? '',
                change.dateTime ? new Date(change.dateTime).toISOString() : '',
                change.updatedPosition ?? '',
                change.change ?? '',
              ].join('|'),
            );
          }
        }

        if (transactionsByTransferId.size > 0) {
          const bulkOps = [...transactionsByTransferId.values()].map((transaction) => ({
            updateOne: {
              filter: { transferId: transaction.transferId },
              update: { $set: transaction },
              upsert: true,
            },
          }));

          try {
            await this.deps.transactionModel.bulkWrite(bulkOps);
          } catch (error) {
            this.deps.logger.error(`Bulk upsert failed for ${this.processName}`, error);
            throw error;
          }
        }

        // Update state id with the last Id
        await this.deps.stateModel.updateOne(
          { process: this.processName },
          { $set: { lastId: newLastId, updatedAt: new Date() } },
          { upsert: true },
        );
        lastId = newLastId;
        waitCount = 0;

        this.deps.logger.info(`Processed up to transferStateChangeId ${lastId}`);
      } catch (error) {
        this.deps.logger.error(`Error in ${this.processName}`, error);
      } finally {
        await new Promise((resolve) => setTimeout(resolve, this.deps.timeout));
      }
    }
  }
}
