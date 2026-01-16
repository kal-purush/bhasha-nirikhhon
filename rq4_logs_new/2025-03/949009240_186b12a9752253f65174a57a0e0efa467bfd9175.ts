import { Account } from '../../domain/models/Account';
import { Transaction } from '../../domain/models/Transaction';

describe('Account', () => {
  let account: Account;

  beforeEach(() => {
    account = new Account();
  });

  it('should start with zero balance', () => {
    // Given
    // An account without initial balance

    // When
    const initialBalance = account.getBalance();

    // Then
    expect(initialBalance).toBe(0);
  });

  it('should deposit money and update balance', () => {
    // Given
    // An account without initial balance

    // When
    account.deposit(100);

    // Then
    expect(account.getBalance()).toBe(100);
    expect(account.getHistory()).toEqual([
      new Transaction(100, expect.any(Date), 'DEPOSIT')
    ]);
  });

  it('should withdraw money and update balance', () => {
    // Given
    account.deposit(100);

    // When
    account.withdraw(50);

    // Then
    expect(account.getBalance()).toBe(50);
    expect(account.getHistory()).toEqual([
      new Transaction(100, expect.any(Date), 'DEPOSIT'),
      new Transaction(-50, expect.any(Date), 'WITHDRAWAL')
    ]);
  });

  it('should maintain correct transaction history', () => {
    // Given
    account.deposit(200);
    account.withdraw(50);
    account.deposit(150);

    // When
    const finalBalance = account.getBalance();
    const history = account.getHistory();

    // Then
    expect(finalBalance).toBe(300);
    expect(history.length).toBe(3);
  });

  it('should not allow withdrawal more than the balance', () => {
    account.deposit(100);
    const withdrawalResult = account.withdraw(150);
    expect(withdrawalResult).toBe(false); // Assuming the method returns false on failure
    expect(account.getBalance()).toBe(100);
    expect(account.getHistory()).toEqual([
      new Transaction(100, expect.any(Date), 'DEPOSIT')
    ]);
  });
  
  it('should not allow deposit of negative amount', () => {
    // Given
    // When
    expect(() => account.deposit(-50)).toThrow("Deposit amount must be greater than zero.");
    expect(account.getBalance()).toBe(0);
    expect(account.getHistory()).toEqual([]);
  });
  
  it('should not allow withdrawal of negative amount', () => {
    account.deposit(100);
    expect(() => account.withdraw(-50)).toThrow("Withdrawal amount must be greater than zero.");
    expect(account.getBalance()).toBe(100);
    expect(account.getHistory()).toEqual([
      new Transaction(100, expect.any(Date), 'DEPOSIT')
    ]);
  });
  
  it('should not affect balance on zero deposit or withdrawal', () => {
    // Given
    // When
    expect(() => account.deposit(0)).toThrow("Deposit amount must be greater than zero.");
    expect(() => account.withdraw(0)).toThrow("Withdrawal amount must be greater than zero.");
    expect(account.getBalance()).toBe(0);
    expect(account.getHistory()).toEqual([]);
  });  
});