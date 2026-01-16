import AWS, { DynamoDB } from 'aws-sdk';
import { environment } from '@config/environment';

AWS.config.update({ ...environment.dynamoDb });

export const docClient = new AWS.DynamoDB.DocumentClient();
export const dynamoDb = new AWS.DynamoDB();

export const query = (params: DynamoDB.QueryInput): Promise<any> => {
  return new Promise((resolve: Function, reject: Function) => {
    docClient.query(params, (err: AWS.AWSError, data: DynamoDB.QueryOutput) => {
      if (err) {
        return reject(err);
      }
      return resolve(data);
    });
  });
};

export const scan = (params: DynamoDB.ScanInput): Promise<any> => {
  return new Promise((resolve: Function, reject: Function) => {
    docClient.scan(params, (err: AWS.AWSError, data: DynamoDB.ScanOutput) => {
      if (err) {
        return reject(err);
      }
      return resolve(data);
    });
  });
};