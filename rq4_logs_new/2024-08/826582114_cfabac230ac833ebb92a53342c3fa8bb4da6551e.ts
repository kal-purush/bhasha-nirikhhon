import { AccountFactory } from '../factory/accounts.factory';
import { AccountType } from '../enums/accounts.type';
import { SavingAccount } from '../model/savingAccount.model';
import { CurrentAccount } from '../model/currentAccount.model';

describe('AccountFactory testing function', () => {

  let randomId = 1
  let specificProperty = 500
  let balance = 500
  test('should create a SavingAccount when type is SAVING', () => {

    const expectResult = AccountFactory.createAccount(
      AccountType.SAVING,
      randomId,
      randomId,
      randomId,
      balance,
      specificProperty
    );

    expect(expectResult).toBeInstanceOf(SavingAccount);
    expect(expectResult.id).toBe(randomId);
    expect(expectResult.idClient).toBe(randomId);
    expect(expectResult.idManager).toBe(randomId);
    expect(expectResult.balance).toBe(balance);
    expect((expectResult as SavingAccount).interest).toBe(specificProperty);
  });

  test('should create a CurrentAccount when type is CURRENT', () => {

    const expectResult = AccountFactory.createAccount(
      AccountType.CURRENT,
      randomId,
      randomId,
      randomId,
      balance,
      specificProperty
    );

    expect(expectResult).toBeInstanceOf(CurrentAccount);
    expect(expectResult.id).toBe(randomId);
    expect(expectResult.idClient).toBe(randomId);
    expect(expectResult.idManager).toBe(randomId);
    expect(expectResult.balance).toBe(balance);
    expect((expectResult as CurrentAccount).limit).toBe(specificProperty);
  });

  test('should throw an error when type is invalid', () => {
    expect(() => {
      AccountFactory.createAccount(
        'invalid_type' as AccountType,
        randomId,
        randomId,
        randomId,
        balance,
        specificProperty
      );
    }).toThrow('Tipo de conta não existe');
  });
});