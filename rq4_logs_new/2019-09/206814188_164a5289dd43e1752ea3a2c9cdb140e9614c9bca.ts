import { DocumentClient } from 'aws-sdk/clients/dynamodb';
import { Expression } from '@typemon/dynamodb-expression';

const accessKeyId = localStorage.getItem('accesskey') || '';
const secretAccessKey = localStorage.getItem('secretkey') || '';
console.log(accessKeyId, secretAccessKey);

interface Option {
    Limit?: number;
    ScanIndexForward?: boolean;
    ExclusiveStartKey?: { [key: string]: any };
}
const client = new DocumentClient({
    region: 'ap-northeast-2',
    accessKeyId,
    secretAccessKey,
});

export const query = async (
    tableName: string,
    keyConditionExpression: Expression,
    filterExpression?: Expression,
    option?: Option,
    index?: string,
): Promise<Array<any>> => {
    try {
        const parameter: DocumentClient.QueryInput = {
            TableName: tableName,
            KeyConditionExpression: keyConditionExpression.expression,
            ExpressionAttributeNames: Object.assign(
                keyConditionExpression.names,
                filterExpression ? filterExpression.names : undefined,
            ),
            ExpressionAttributeValues: Object.assign(
                keyConditionExpression.values,
                filterExpression ? filterExpression.values : undefined,
            ),
            FilterExpression: filterExpression ? filterExpression.expression : undefined,
            IndexName: index ? index : undefined,
            ...option,
        };
        console.log(parameter);
        const result = await client.query(parameter).promise();
        return result.Items as any;
    } catch (error) {
        console.log('query', error);
        throw new Error(error);
    }
};