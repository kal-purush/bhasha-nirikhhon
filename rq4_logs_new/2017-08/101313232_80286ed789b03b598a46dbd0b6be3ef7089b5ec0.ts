export interface DebtEntity {
    _id: string,
    amount: number,
    timestamp: number,
    reason: string,
    creditor_id: string,
    creditor_firstName: string,
    creditor_lastName: string,
    creditor_photoUrl: string,
    creditor_sub: string,
    debtor_id: string,
    debtor_firstName: string,
    debtor_lastName: string,
    debtor_photoUrl: string,
    debtor_sub: string
}