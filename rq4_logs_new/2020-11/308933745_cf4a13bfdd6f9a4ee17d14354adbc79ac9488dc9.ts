import { APIGatewayProxyHandler } from 'aws-lambda';
import ProductService from '../services/product.service';

export const addNewProduct: APIGatewayProxyHandler = async (event, _context) => {
    const response = {
        statusCode: 200,
        headers: {
            'Access-Control-Allow-Origin': '*'
        },
        body: ''
    };

    console.log('addNewProduct lambda event:', JSON.stringify(event));

    if (typeof event.body === 'string') {
        event.body = JSON.parse(event.body);
    }

    try {
        const { title, description, img, price, count } = <any>event.body;
        await ProductService.addNewProduct({ title, description, img, price, count });
    } catch (error) {
        response.statusCode = 500;
        response.body = JSON.stringify(error, null, 2);
    }
    return response;
}