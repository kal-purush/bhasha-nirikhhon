import { DynamoDB } from 'aws-sdk';

export type DynamoPaginationKey = {
  pk: string,
  sk: string
  limit?: number
}

export const ConvertToDynamoKey = (input: DynamoPaginationKey) : DynamoDB.Key => ({
  pk: {
    S: input.pk,
  },
  sk: {
    S: input.sk,
  },
});

export const GetDynamoPaginationKey = (pageKey: DynamoDB.Key) : DynamoPaginationKey => ({
  pk: pageKey.pk.S as string,
  sk: pageKey.sk.S as string,
});