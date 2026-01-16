import { AccountRepository } from '../../resources/accounts/account.repository';
import { AccountService } from '../../resources/accounts/account.service';
import { AccountDto } from '../../resources/accounts/interfaces/account.interface';
import { KafkaProducer } from '../kafka/kafka.producer';

const kafkaProducer = new KafkaProducer();
const accountRepository = new AccountRepository();
const accountService = new AccountService(accountRepository, kafkaProducer);

export const sendPayment = async () => {
  const accounts: AccountDto[] = await accountService.all();

  const makePayment = async (account: AccountDto) => {
    if (account.balance > 0) {
      const accountBalance = account.balance;
      console.log(`Make payment to ${account.user_id}`);
      account.balance = 0;
      await accountService.updateBalance(account.id, accountBalance);

      console.log(`Notify ${account.user_id} about payment`);
    }
  };

  const paymentPromises = accounts.map(makePayment);
  await Promise.all(paymentPromises);
};