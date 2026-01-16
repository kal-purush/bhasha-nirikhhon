import { AccountRepository } from '../../../resources/accounts/account.repository';
import { TransactionRepository } from '../../../resources/transactions/transaction.repository';
import { TransactionService } from '../../../resources/transactions/transaction.service';

const accountsRepository = new AccountRepository();
const transactionRepository = new TransactionRepository();
const transactionService = new TransactionService(transactionRepository, accountsRepository);

export const taskTransactionHandler = async message => {
  const { event_version } = message.headers;

  if (event_version.toString() !== '1') {
    throw new Error('Unsupported version');
  }

  switch (message.key.toString()) {
    case 'TaskAssigned':
      return await transactionService.create(message.value);
    case 'TaskCompleted':
      return await transactionService.create(message.value);
    default:
      return;
  }
};