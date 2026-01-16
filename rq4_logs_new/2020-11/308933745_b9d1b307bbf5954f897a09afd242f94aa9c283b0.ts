import { SQSEvent, SQSHandler } from 'aws-lambda';
import * as AWS from 'aws-sdk';
import ProductService from '../services/product.service';

export const catalogBatchProcess: SQSHandler = (event: SQSEvent): void => {
    const sns = new AWS.SNS({ region: 'eu-west-1'});
    const products = event.Records.map(({ body }) => JSON.parse(body));
    try {
        products.forEach(async (p) => {
            await ProductService.addNewProduct(p);
        });
        sns.publish({
            TopicArn: process.env.SNS_ARN,
            Subject: 'AWS_SNS_SQS_TASK_6',
            Message: `${products.length} products are added`
        }, err => {
            if(err) {
                console.error('[catalogBatchProcess]: email sending was failed -', err);
                return;
            }
            console.log('[catalogBatchProcess]: sending message on email');
        });
    } catch (error) {
        console.error('[catalogBatchProcess]:', error);
    }
}