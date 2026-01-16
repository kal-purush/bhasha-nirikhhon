import { gql } from "graphql-request";

export const QueryUserTradeHistory = gql`
    query UserTradeHistories(
        $where: UserTradeHistory_filter!
        $limit: Int!
        $skip: Int!
        $direction: OrderDirection!
        $sortBy: UserTradeHistory_orderBy!
    ) {
        userTradeHistories(where: $where, first: $limit, orderBy: $sortBy, orderDirection: $direction, skip: $skip) {
            account
            action
            amountMileage
            amountToken
            balanceMileage
            balanceToken
            blockNumber
            blockTimestamp
            email
            franchiseeId
            id
            purchaseId
            transactionHash
            value
        }
    }
`;