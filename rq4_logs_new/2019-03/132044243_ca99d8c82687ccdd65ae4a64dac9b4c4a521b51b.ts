export interface Amount {value: number, currency: string};

export interface Balance { [currency: string] : Amount}

export abstract class Total {
    static total(amount: any): Balance {
        let balance: Balance = {};
        return Total.add(balance, amount);
    }
    static add(balance: Balance, amount: any): Balance {
        if (amount instanceof Array) {
            for(let a of amount) {
                Total.add(balance, a);
            }
        } else if ('accounts' in amount) {
            Total.add(balance, amount.accounts);
        } else {
            let currency: string;
            let value: number;
            if ('currency' in amount) {
                currency = amount.currency;
            }
            if ('value' in amount) {
                value = amount.value;
            }
            if ('balance' in amount) {
                value = amount.balance;
            }
            if (currency && value) {
                let a = balance[currency] || { value:0, currency:currency};
                a.value += value;
                balance[currency] = a;
            }    
        }
        return balance;
    }
}