import { SQSEvent, SQSHandler } from 'aws-lambda';
import 'source-map-support/register';
import { invoke } from '../sql/db_client.helper';
import { ProductSchema } from '../models/Product'
import { headers } from '../enums/constants';
import * as AWS from 'aws-sdk';
const { PRODUCT_SERVICE_REGION, TOPIC_ARN } = process.env;

export const catalogBatchProcess: SQSHandler = async (event: SQSEvent): Promise<void> => {
    AWS.config.update({ region: PRODUCT_SERVICE_REGION });
    const data = event.Records;
    console.log(data)
    // Create publish parameters
    const params = {
        Message: 'MESSAGE_TEXT', /* required */
        TopicArn: TOPIC_ARN
    };
    console.log(TOPIC_ARN)
    // Create promise and SNS service object
    const publishTextPromise = new AWS.SNS().publish(params).promise();

    // Handle promise's fulfilled/rejected states
    const data_1 = await publishTextPromise;
    console.log(`Message ${params.Message} sent to the topic ${params.TopicArn}`);
    console.log("MessageID is " + data_1.MessageId);
    // try {
    //     await ProductSchema.validate(data);
    //     const { title, price = 0, description = '', count = 0 } = data;
    //     const { rows: [{ id }] } = await invoke('INSERT INTO products (title, price, description) VALUES ($1, $2, $3) RETURNING id',
    //         [title, price, description]
    //     );
    //     await invoke('INSERT INTO stocks (product_id, count) VALUES ($1, $2)',
    //         [id, count]
    //     );
    //     return {
    //         headers,
    //         statusCode: 200,
    //         body: JSON.stringify(`Product [${id}] was added successfully!`)
    //     }
    // } catch (error) {
    //     const msesage = error.errorMessage ? error.errorMessage : error.message;
    //     console.error(msesage);
    //     return {
    //         headers,
    //         statusCode: 500,
    //         body: JSON.stringify({ message: `SERVER_ERROR: [${error}]` }, null, 2)
    //     }
    // }
};