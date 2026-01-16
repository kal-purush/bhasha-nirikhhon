const currencies = require('js-money/lib/currency');

interface USD {
    "symbol": "$",
    "name": "US Dollar",
    "symbol_native": "$",
    "decimal_digits": 2,
    "rounding": 0,
    "code": "USD",
    "name_plural": "US dollars"
}

interface JPY {
    "symbol": "¥",
    "name": "Japanese Yen",
    "symbol_native": "￥",
    "decimal_digits": 0,
    "rounding": 0,
    "code": "JPY",
    "name_plural": "Japanese yen"
}

export interface MoneyJSON {
    amount: number;
    currency: CurrencyType;
}

export type Currency = USD | JPY;
export type CurrencyType = 'USD' | 'JPY';

class BaseCurrency {
    public static USD: USD;
    public static JPY: JPY
}

Object.assign(BaseCurrency, currencies);

export function isCurrency(object: any): object is Currency {
    return object && typeof object.symbol === 'string' && typeof object.code === 'string';
}

export function isMoney(object: any): object is Money {
    return object && typeof object.amount === 'number' && typeof object.currency === 'string';
}

export class Money extends BaseCurrency {
    amount: number;
    currency: CurrencyType;

    constructor(amount: number, currency: Currency | CurrencyType) {
        super();
        this.amount = amount;
        this.currency = isCurrency(currency) ? currency.code : currency;
    }

    add(money: Money): Money {
        return new Money(this.amount + money.amount, this.currency);
    }

    subtract(money: Money): Money {
        return new Money(this.amount - money.amount, this.currency);
    }

    multiply(multiplier: number, fn: typeof Math.ceil | typeof Math.floor | typeof Math.round = Math.round): Money {
        const amount = fn(this.amount * multiplier);
        return new Money(amount, this.currency);
    }

    divide(divisor: number, fn: typeof Math.ceil | typeof Math.floor | typeof Math.round = Math.round): Money {
        const amount = fn(this.amount / divisor);
        return new Money(amount, this.currency);
    }

    equals(money: Money): boolean {
        return this.amount === money.amount && this.currency === money.currency;
    }

    compare(money: Money): -1 | 0 | 1 {
        if (this.amount === money.amount) {
            return 0;
        }

        return this.amount > money.amount ? 1 : -1;
    }

    greaterThan(money: Money): boolean {
        return 1 === this.compare(money);
    }

    greaterThanOrEqual(money: Money): boolean {
        return 0 <= this.compare(money);
    }

    lessThan(money: Money): boolean {
        return -1 === this.compare(money);
    }

    lessThanOrEqual(money: Money): boolean {
        return 0 >= this.compare(money);
    }
}