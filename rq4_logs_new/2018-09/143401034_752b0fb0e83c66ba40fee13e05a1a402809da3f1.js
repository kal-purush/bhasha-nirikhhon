import _ from 'lodash';
import { RESPONSE_STATUS_SUCCESS, RESPONSE_STATUS_ERROR } from 'constants/messageStatuses';
import { WEB3_ACTION_NOTIFICATION_TRANSFER } from 'constants/messageActions';

let transactionsList = {};
let waitForUnlockRequests = [];
let isForceTransactionsChecking = false;
let module = null;

export default class TransferController {

    constructor(baseModule) {
        module = baseModule;
    }

    getTransfersInProgress({request, response}) {
        const { userIds } = request;
        const transactions = [];

        _.forEach(this.transactions, (transaction) => {
            if (userIds.includes(transaction[Symbol.for('userId')])) {
                transactions.push(transaction[Symbol.for('status')]);
            }
        });

        response.data.status = RESPONSE_STATUS_SUCCESS;
        response.data.response = { transactions };
        response.data.response = { transactions };

        module.sendResponseToClient(response);
    }

    checkTransactions({number}) {
        let needToForceCheck = false;
        let index = 0;

        _.forEach(this.transactions, (transaction) => {
            typeof transaction[Symbol.for('checkTransaction')] === 'function' && transaction[Symbol.for('checkTransaction')](number);
            const diff = 50 + index / 10;

            if (!transaction[Symbol.for('blockDiff')]) {
                transaction[Symbol.for('blockDiff')] = diff;
            }

            needToForceCheck = number - transaction.blockNumber >= transaction[Symbol.for('blockDiff')];
            index++;
        });

        console.log(needToForceCheck);
        if (needToForceCheck) {

            this.forceCheckFailedTransactions(number);
        }
    }

    forceCheckFailedTransactions(blockNumber) {
        if (isForceTransactionsChecking) {
            return false;
        }

        isForceTransactionsChecking = true;
        console.info('isForceTransactionsChecking', isForceTransactionsChecking);
        let amount = Object.keys(this.transactions).length;
        _.forEach(this.transactions, async (transaction) => {
            const { transactionHash } = transaction;
            const transactionReceipt = await getTransactionReceipt(transactionHash);
console.log(transactionReceipt, blockNumber, transaction.blockNumber, transaction[Symbol.for('blockDiff')]);
            if (
                transactionReceipt && !Number(_.get(transactionReceipt, 'status')) ||
                transactionReceipt === null && blockNumber && blockNumber - transaction.blockNumber >= transaction[Symbol.for('blockDiff')]
            ) {
                transaction.status = 0;
                notifyTransfersInProgress(transaction);
                this.removeTransaction(transaction);
            }

            if (--amount === 0) {
                isForceTransactionsChecking = false;
                console.info('isForceTransactionsChecking', isForceTransactionsChecking);
            }
        });


    }

    removeTransaction({transactionHash}) {

        if (!_.isEmpty(transactionsList[transactionHash])) {
            delete transactionsList[transactionHash];
        }

    }

    isTransactionExist({transactionHash}) {
        return !_.isEmpty(transactionsList[transactionHash]);
    }

    clearWaitForUnlockRequests() {
        waitForUnlockRequests = [];
    }

    set updateTransactionMined(transaction) {
        const { transactionHash } = transaction;

        if (!_.isEmpty(transactionsList[transactionHash])) {
            transactionsList[transactionHash] = transaction;
        }
    }

    set transactions(transaction) {
        const { transactionHash } = transaction;

        if (_.isEmpty(transactionsList[transaction])) {
            transactionsList[transactionHash] = transaction;
        }
        console.info('Transactions amount: ', Object.keys(transactionsList).length);
    }

    get transactions() {
        return transactionsList;
    }

    get waitForUnlockRequests() {
        return waitForUnlockRequests;
    }

    set waitForUnlockRequests(request) {
        waitForUnlockRequests.push(request);
    }
}

export function getTransactionReceipt(transactionId) {
    return new Promise((resolve) => {
        module.eth.getTransactionReceipt(transactionId).then((response) => {
            resolve(response);
        }).catch((e) => {
            resolve(false);
            console.error(e);
        });
    });
}

export function notifyTransfersInProgress(transaction) {
    const response = {
        userId: transaction[Symbol.for('userId')],
        data: {
            status: Number(transaction.status) ? RESPONSE_STATUS_SUCCESS : RESPONSE_STATUS_ERROR,
            command: WEB3_ACTION_NOTIFICATION_TRANSFER,
            response: transaction[Symbol.for('status')]
        }
    };

    module.sendResponseToClients(response);
}