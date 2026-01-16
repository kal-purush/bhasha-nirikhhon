class Account {
    private transactions: Transaction[] = [];

    deposit(amount: number) {
        this.transactions.push(new Transaction(amount, new Date(), 'DEPOSIT'));
    }

    withdraw(amount: number) {
        this.transactions.push(new Transaction(-amount, new Date(), 'WITHDRAWAL'));
    }

    getBalance(): number {
        return this.transactions.reduce((acc, transaction) => acc + transaction.amount, 0);
    }

    getHistory(): Transaction[] {
        return this.transactions;
    }
}