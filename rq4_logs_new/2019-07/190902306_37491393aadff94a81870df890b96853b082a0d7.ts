/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: CompanyQuery
// ====================================================

export interface CompanyQuery_company {
    id: string
    name: string
    logo: string
    description: string
}

export interface CompanyQuery {
    company: CompanyQuery_company | null
}

export interface CompanyQueryVariables {
    id: string
}