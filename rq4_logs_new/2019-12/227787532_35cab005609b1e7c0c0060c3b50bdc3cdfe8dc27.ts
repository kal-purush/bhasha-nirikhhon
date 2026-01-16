/* tslint:disable */
//  This file was automatically generated and should not be edited.
import { Injectable } from "@angular/core";
import API, { graphqlOperation } from "@aws-amplify/api";
import { GraphQLResult } from "@aws-amplify/api/lib/types";
import * as Observable from "zen-observable";

export type CreateHeroInput = {
  id?: string | null;
  name: string;
  power?: string | null;
  status?: boolean | null;
};

export type ModelHeroConditionInput = {
  name?: ModelStringInput | null;
  power?: ModelStringInput | null;
  status?: ModelBooleanInput | null;
  and?: Array<ModelHeroConditionInput | null> | null;
  or?: Array<ModelHeroConditionInput | null> | null;
  not?: ModelHeroConditionInput | null;
};

export type ModelStringInput = {
  ne?: string | null;
  eq?: string | null;
  le?: string | null;
  lt?: string | null;
  ge?: string | null;
  gt?: string | null;
  contains?: string | null;
  notContains?: string | null;
  between?: Array<string | null> | null;
  beginsWith?: string | null;
  attributeExists?: boolean | null;
  attributeType?: ModelAttributeTypes | null;
  size?: ModelSizeInput | null;
};

export enum ModelAttributeTypes {
  binary = "binary",
  binarySet = "binarySet",
  bool = "bool",
  list = "list",
  map = "map",
  number = "number",
  numberSet = "numberSet",
  string = "string",
  stringSet = "stringSet",
  _null = "_null"
}

export type ModelSizeInput = {
  ne?: number | null;
  eq?: number | null;
  le?: number | null;
  lt?: number | null;
  ge?: number | null;
  gt?: number | null;
  between?: Array<number | null> | null;
};

export type ModelBooleanInput = {
  ne?: boolean | null;
  eq?: boolean | null;
  attributeExists?: boolean | null;
  attributeType?: ModelAttributeTypes | null;
};

export type UpdateHeroInput = {
  id: string;
  name?: string | null;
  power?: string | null;
  status?: boolean | null;
};

export type DeleteHeroInput = {
  id?: string | null;
};

export type ModelHeroFilterInput = {
  id?: ModelIDInput | null;
  name?: ModelStringInput | null;
  power?: ModelStringInput | null;
  status?: ModelBooleanInput | null;
  and?: Array<ModelHeroFilterInput | null> | null;
  or?: Array<ModelHeroFilterInput | null> | null;
  not?: ModelHeroFilterInput | null;
};

export type ModelIDInput = {
  ne?: string | null;
  eq?: string | null;
  le?: string | null;
  lt?: string | null;
  ge?: string | null;
  gt?: string | null;
  contains?: string | null;
  notContains?: string | null;
  between?: Array<string | null> | null;
  beginsWith?: string | null;
  attributeExists?: boolean | null;
  attributeType?: ModelAttributeTypes | null;
  size?: ModelSizeInput | null;
};

export type CreateHeroMutation = {
  __typename: "Hero";
  id: string;
  name: string;
  power: string | null;
  status: boolean | null;
};

export type UpdateHeroMutation = {
  __typename: "Hero";
  id: string;
  name: string;
  power: string | null;
  status: boolean | null;
};

export type DeleteHeroMutation = {
  __typename: "Hero";
  id: string;
  name: string;
  power: string | null;
  status: boolean | null;
};

export type GetHeroQuery = {
  __typename: "Hero";
  id: string;
  name: string;
  power: string | null;
  status: boolean | null;
};

export type ListHerosQuery = {
  __typename: "ModelHeroConnection";
  items: Array<{
    __typename: "Hero";
    id: string;
    name: string;
    power: string | null;
    status: boolean | null;
  } | null> | null;
  nextToken: string | null;
};

export type OnCreateHeroSubscription = {
  __typename: "Hero";
  id: string;
  name: string;
  power: string | null;
  status: boolean | null;
};

export type OnUpdateHeroSubscription = {
  __typename: "Hero";
  id: string;
  name: string;
  power: string | null;
  status: boolean | null;
};

export type OnDeleteHeroSubscription = {
  __typename: "Hero";
  id: string;
  name: string;
  power: string | null;
  status: boolean | null;
};

@Injectable({
  providedIn: "root"
})
export class APIService {
  async CreateHero(
    input: CreateHeroInput,
    condition?: ModelHeroConditionInput
  ): Promise<CreateHeroMutation> {
    const statement = `mutation CreateHero($input: CreateHeroInput!, $condition: ModelHeroConditionInput) {
        createHero(input: $input, condition: $condition) {
          __typename
          id
          name
          power
          status
        }
      }`;
    const gqlAPIServiceArguments: any = {
      input
    };
    if (condition) {
      gqlAPIServiceArguments.condition = condition;
    }
    const response = (await API.graphql(
      graphqlOperation(statement, gqlAPIServiceArguments)
    )) as any;
    return <CreateHeroMutation>response.data.createHero;
  }
  async UpdateHero(
    input: UpdateHeroInput,
    condition?: ModelHeroConditionInput
  ): Promise<UpdateHeroMutation> {
    const statement = `mutation UpdateHero($input: UpdateHeroInput!, $condition: ModelHeroConditionInput) {
        updateHero(input: $input, condition: $condition) {
          __typename
          id
          name
          power
          status
        }
      }`;
    const gqlAPIServiceArguments: any = {
      input
    };
    if (condition) {
      gqlAPIServiceArguments.condition = condition;
    }
    const response = (await API.graphql(
      graphqlOperation(statement, gqlAPIServiceArguments)
    )) as any;
    return <UpdateHeroMutation>response.data.updateHero;
  }
  async DeleteHero(
    input: DeleteHeroInput,
    condition?: ModelHeroConditionInput
  ): Promise<DeleteHeroMutation> {
    const statement = `mutation DeleteHero($input: DeleteHeroInput!, $condition: ModelHeroConditionInput) {
        deleteHero(input: $input, condition: $condition) {
          __typename
          id
          name
          power
          status
        }
      }`;
    const gqlAPIServiceArguments: any = {
      input
    };
    if (condition) {
      gqlAPIServiceArguments.condition = condition;
    }
    const response = (await API.graphql(
      graphqlOperation(statement, gqlAPIServiceArguments)
    )) as any;
    return <DeleteHeroMutation>response.data.deleteHero;
  }
  async GetHero(id: string): Promise<GetHeroQuery> {
    const statement = `query GetHero($id: ID!) {
        getHero(id: $id) {
          __typename
          id
          name
          power
          status
        }
      }`;
    const gqlAPIServiceArguments: any = {
      id
    };
    const response = (await API.graphql(
      graphqlOperation(statement, gqlAPIServiceArguments)
    )) as any;
    return <GetHeroQuery>response.data.getHero;
  }
  async ListHeros(
    filter?: ModelHeroFilterInput,
    limit?: number,
    nextToken?: string
  ): Promise<ListHerosQuery> {
    const statement = `query ListHeros($filter: ModelHeroFilterInput, $limit: Int, $nextToken: String) {
        listHeros(filter: $filter, limit: $limit, nextToken: $nextToken) {
          __typename
          items {
            __typename
            id
            name
            power
            status
          }
          nextToken
        }
      }`;
    const gqlAPIServiceArguments: any = {};
    if (filter) {
      gqlAPIServiceArguments.filter = filter;
    }
    if (limit) {
      gqlAPIServiceArguments.limit = limit;
    }
    if (nextToken) {
      gqlAPIServiceArguments.nextToken = nextToken;
    }
    const response = (await API.graphql(
      graphqlOperation(statement, gqlAPIServiceArguments)
    )) as any;
    return <ListHerosQuery>response.data.listHeros;
  }
  OnCreateHeroListener: Observable<OnCreateHeroSubscription> = API.graphql(
    graphqlOperation(
      `subscription OnCreateHero {
        onCreateHero {
          __typename
          id
          name
          power
          status
        }
      }`
    )
  ) as Observable<OnCreateHeroSubscription>;

  OnUpdateHeroListener: Observable<OnUpdateHeroSubscription> = API.graphql(
    graphqlOperation(
      `subscription OnUpdateHero {
        onUpdateHero {
          __typename
          id
          name
          power
          status
        }
      }`
    )
  ) as Observable<OnUpdateHeroSubscription>;

  OnDeleteHeroListener: Observable<OnDeleteHeroSubscription> = API.graphql(
    graphqlOperation(
      `subscription OnDeleteHero {
        onDeleteHero {
          __typename
          id
          name
          power
          status
        }
      }`
    )
  ) as Observable<OnDeleteHeroSubscription>;
}