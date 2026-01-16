/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { PriceTier } from "./../../../../@types/__generated__/globalTypes";

// ====================================================
// GraphQL fragment: HarborTiers
// ====================================================

export interface HarborTiers_edges_node_properties_piers_edges_node_properties {
  __typename: "PierProperties";
  priceTier: PriceTier | null;
}

export interface HarborTiers_edges_node_properties_piers_edges_node {
  __typename: "PierNode";
  id: string;
  properties: HarborTiers_edges_node_properties_piers_edges_node_properties | null;
}

export interface HarborTiers_edges_node_properties_piers_edges {
  __typename: "PierNodeEdge";
  node: HarborTiers_edges_node_properties_piers_edges_node | null;
}

export interface HarborTiers_edges_node_properties_piers {
  __typename: "PierNodeConnection";
  edges: (HarborTiers_edges_node_properties_piers_edges | null)[];
}

export interface HarborTiers_edges_node_properties {
  __typename: "HarborProperties";
  name: string | null;
  piers: HarborTiers_edges_node_properties_piers | null;
}

export interface HarborTiers_edges_node {
  __typename: "HarborNode";
  id: string;
  properties: HarborTiers_edges_node_properties | null;
}

export interface HarborTiers_edges {
  __typename: "HarborNodeEdge";
  node: HarborTiers_edges_node | null;
}

export interface HarborTiers {
  __typename: "HarborNodeConnection";
  edges: (HarborTiers_edges | null)[];
}