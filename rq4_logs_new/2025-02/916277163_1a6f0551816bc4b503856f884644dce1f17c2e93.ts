interface IFinanceBase {
    id: string;
    name: string;
    created_at: Date;
    updated_at: Date;
    deleted_at: Date;
}

export type ISupplierType = IFinanceBase;

export interface ISupplierCategory extends IFinanceBase {
    type: ISupplierType;
}

export type ITypePaymentMethod = IFinanceBase;

export interface IPaymentMethod extends IFinanceBase {
    type: ITypePaymentMethod;
}

export interface ISupplier extends IFinanceBase {
    category: ISupplierCategory;
    description?: string;
}

export type TExpense = 'FIXED' | 'VARIABLE';
export type TStatusExpense = 'PENDING' | 'PAID' | 'CANCELLED' | 'REFUNDED' | 'OVERDUE' | 'EXPIRED' | 'NOT PAID';

export type TMonth = 'JANUARY'| 'FEBRUARY'| 'MARCH'| 'APRIL'| 'MAY'| 'JUNE'| 'JULY'| 'AUGUST'| 'SEPTEMBER'| 'OCTOBER'| 'NOVEMBER'| 'DECEMBER';

export interface IExpense extends IFinanceBase {
    type: TExpense;
    value: number;
    date: Date;
    months: Array<TMonth>;
    status: TStatusExpense;
    supplier: ISupplier;
    description?: string;
    payment_method: IPaymentMethod;
}