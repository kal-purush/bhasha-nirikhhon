import { APIGatewayProxyHandler } from 'aws-lambda';
import 'source-map-support/register';
import * as invoke from '../sql/db_client.helper';
import { getPattern } from '../enums/patterns.enum';
import { headers } from '../enums/constants';


export const deleteProductsById: APIGatewayProxyHandler = async (event, _context) => {
    let product;
    let response = {};
    console.log(headers);
    try {
        const req_id = event.pathParameters.productId;
        if (!getPattern("UUID_V4").test(req_id)) throw new Error("ProductId should match to the UUID pattern.")
        product = await invoke.invoke(`select p.id, p.description, p.price, p.title, s.count from products p left join stocks s on p.id = s.product_id where s.product_id = '${req_id}' and p.id = '${req_id}'`);
        // console.debug(product.rows[0]);
        response = (product.rows[0] ?
            {
                headers,
                statusCode: 200,
                body: JSON.stringify(product.rows[0])
            }
            :
            {
                headers,
                statusCode: 404,
                body: JSON.stringify({ message: `Prouct not found: [${req_id}]` }, null, 2)
            });
    } catch (error) {
        response = {
            headers,
            statusCode: 500,
            body: JSON.stringify({ message: `SERVER_ERROR: [${error}]` }, null, 2)
        };
    } finally {
        return response;
    }
}