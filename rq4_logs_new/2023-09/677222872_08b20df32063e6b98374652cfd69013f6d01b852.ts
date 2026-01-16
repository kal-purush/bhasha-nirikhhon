import { gql } from "graphql-request";

export const QueryPaidPoint = gql`
    query PaidPoint($where: PaidPoint_filter!) {
        paidPoints(where: $where) {
            id
            email
            paidAmountPoint
            value
            balancePoint
            purchaseId
            shopId
            blockNumber
            blockTimestamp
            transactionHash
        }
    }
`;