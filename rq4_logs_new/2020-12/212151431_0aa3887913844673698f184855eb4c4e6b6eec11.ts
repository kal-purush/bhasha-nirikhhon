/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { ContractStatus } from "./../../../../../@types/__generated__/globalTypes";

// ====================================================
// GraphQL query operation: WINTER_STORAGE_CONTRACT
// ====================================================

export interface WINTER_STORAGE_CONTRACT_winterStorageLease_contract {
  __typename: "WinterStorageContractNode";
  createdAt: any;
  modifiedAt: any;
  status: ContractStatus;
}

export interface WINTER_STORAGE_CONTRACT_winterStorageLease_order {
  __typename: "OrderNode";
  orderNumber: string;
}

export interface WINTER_STORAGE_CONTRACT_winterStorageLease {
  __typename: "WinterStorageLeaseNode";
  contract: WINTER_STORAGE_CONTRACT_winterStorageLease_contract | null;
  order: WINTER_STORAGE_CONTRACT_winterStorageLease_order | null;
}

export interface WINTER_STORAGE_CONTRACT {
  winterStorageLease: WINTER_STORAGE_CONTRACT_winterStorageLease | null;
}

export interface WINTER_STORAGE_CONTRACTVariables {
  leaseId: string;
}