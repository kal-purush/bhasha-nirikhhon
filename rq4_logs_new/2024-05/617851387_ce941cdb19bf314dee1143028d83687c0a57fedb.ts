import Organization from '../../../../props/models/Organization';

interface ProviderFinancialTransaction {
    id: number;
    amount: string;
    amount_locale: string;
    provider?: string;
}

interface ProviderFinancialDailyTransaction {
    date?: string;
    amount?: string;
    amount_locale?: string;
    date_locale?: string;
}

export interface FinancialFilterOptionItem {
    id?: number;
    name: string;
    checked?: boolean;
    transactions?: number;
}

export interface ProviderFinancialFilterOptions {
    funds: Array<FinancialFilterOptionItem>;
    postcodes: Array<FinancialFilterOptionItem>;
    providers: Array<FinancialFilterOptionItem>;
    business_types: Array<FinancialFilterOptionItem>;
    product_categories: Array<FinancialFilterOptionItem>;
}

export interface ProviderFinancialStatistics {
    dates: Array<{
        key: string;
        count: number;
        amount?: string;
        lowest_transaction?: ProviderFinancialTransaction;
        highest_transaction?: ProviderFinancialTransaction;
        highest_daily_transaction?: ProviderFinancialDailyTransaction;
    }>;
    totals: {
        count: string;
        amount: string;
        amount_locale: string;
    };
    filters: ProviderFinancialFilterOptions;
    lowest_transaction?: ProviderFinancialTransaction;
    highest_transaction?: ProviderFinancialTransaction;
    highest_daily_transaction?: ProviderFinancialDailyTransaction;
}

export interface ProviderFinancial {
    provider: Organization;
    total_spent?: string;
    total_spent_locale?: string;
    highest_transaction?: string;
    highest_transaction_locale?: string;
    nr_transactions?: number;
}

interface FundTotals {
    budget: number;
    budget_left: number;
    budget_used: number;
    budget_locale: string;
    budget_left_locale: string;
    budget_used_locale: string;
    budget_used_active_vouchers: number;
    budget_used_active_vouchers_locale: string;
    transaction_costs: number;
    transaction_costs_locale: string;
    vouchers_count: number;
    vouchers_amount: string;
    vouchers_amount_locale: string;
    active_vouchers_amount: string;
    active_vouchers_amount_locale: string;
    active_vouchers_count: number;
    inactive_vouchers_amount: string;
    inactive_vouchers_amount_locale: string;
    inactive_vouchers_count: number;
    deactivated_vouchers_amount: string;
    deactivated_vouchers_amount_locale: string;
    deactivated_vouchers_count: number;
}

export interface FinancialOverview {
    funds: FundTotals;
    budget_funds: FundTotals;
}