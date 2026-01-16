import { AccountRepository } from '../../../resources/accounts/account.repository';
import { AccountService } from '../../../resources/accounts/account.service';
import { KafkaProducer } from '../kafka.producer';

const accountsRepository = new AccountRepository();
const kafkaProducer = new KafkaProducer();
const accountService = new AccountService(accountsRepository, kafkaProducer);

export const taskTransactionHandler = async message => {
  const { event_version } = message.headers;

  if (event_version.toString() !== '1') {
    throw new Error('Unsupported version');
  }

  switch (message.key.toString()) {
    case 'TaskAssigned':
      return await accountService.addTransaction('credit', message.value);
    case 'TaskCompleted':
      return await accountService.addTransaction('debit', message.value);
    default:
      return;
  }
};