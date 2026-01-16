/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { ContractStatus } from "./../../../../../@types/__generated__/globalTypes";

// ====================================================
// GraphQL query operation: BERTH_CONTRACT
// ====================================================

export interface BERTH_CONTRACT_berthLease_contract {
  __typename: "BerthContractNode";
  createdAt: any;
  modifiedAt: any;
  status: ContractStatus | null;
}

export interface BERTH_CONTRACT_berthLease_order {
  __typename: "OrderNode";
  orderNumber: string;
}

export interface BERTH_CONTRACT_berthLease {
  __typename: "BerthLeaseNode";
  contract: BERTH_CONTRACT_berthLease_contract | null;
  order: BERTH_CONTRACT_berthLease_order | null;
}

export interface BERTH_CONTRACT {
  berthLease: BERTH_CONTRACT_berthLease | null;
}

export interface BERTH_CONTRACTVariables {
  leaseId: string;
}