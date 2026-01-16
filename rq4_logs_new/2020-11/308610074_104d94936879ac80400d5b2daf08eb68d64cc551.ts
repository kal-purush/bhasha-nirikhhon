import { APIGatewayProxyHandler } from 'aws-lambda';
import 'source-map-support/register';
import { invoke } from '../sql/db_client.helper';
import { ProductSchema } from '../models/Product'
import { headers } from '../enums/constants';

export const addProduct: APIGatewayProxyHandler = async (event) => {
    const data = JSON.parse(event.body);
    try {
        await ProductSchema.validate(data);
        const { title, price = 0, description = '', count = 0 } = data;
        const { rows: [{ id }] } = await invoke('INSERT INTO products (title, price, description) VALUES ($1, $2, $3) RETURNING id',
            [title, price, description]
        );
        await invoke('INSERT INTO stocks (product_id, count) VALUES ($1, $2)',
            [id, count]
        );
        return {
            headers,
            statusCode: 200,
            body: JSON.stringify(`Product [${id}] was added successfully!`)
        }
    } catch (error) {
        const msesage = error.errorMessage ? error.errorMessage : error.message;
        console.error(msesage);
        return {
            headers,
            statusCode: 500,
            body: JSON.stringify({ message: `SERVER_ERROR: [${error}]` }, null, 2)
        }
    }
};