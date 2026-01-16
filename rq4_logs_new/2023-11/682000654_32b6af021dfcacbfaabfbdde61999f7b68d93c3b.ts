import { gql } from 'apollo-angular';
import {
  DeleteTransactionResponse,
  DeleteTransactionVariables,
} from '../types/delete-transaction.types';

export const DELETE_TRANSACTION = gql<
  DeleteTransactionResponse,
  DeleteTransactionVariables
>`
  mutation deleteTransaction($id: ID) {
    deleteTransaction(id: $id)
  }
`;