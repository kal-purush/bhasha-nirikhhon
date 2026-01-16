import { IRouteSource } from 'interfaces/route';
import ProductModel from 'app/models/Product';
import { OK, NOT_FOUND } from 'http-status';

interface IProductsController {
    list: ({ req, res, app }: IRouteSource) => Promise<any>;
}

class ProductsController implements IProductsController {
    /**
     * @param IRouteSource
     * @returns Promise<any>
     */
    async list({
        req, res, next, app,
    }: IRouteSource): Promise<any> {
        const products = await ProductModel.find();

        if (!products.length) res.status(NOT_FOUND).json({ message: 'products not found.' });

        return res.status(OK).json({ products });
    }
}

export default ProductsController;