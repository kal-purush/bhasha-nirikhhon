import { Request, Response } from 'express';
import ProductService from '../services/ProductService';

class ProductController {
	private productService: ProductService;

	constructor() {
		this.productService = new ProductService();
	}

	public async insertProduct(req: Request, res: Response): Promise<void> {
		try {
			console.log(req.body);
			await this.productService.insertProduct(req.body);
			res.status(201).json({ message: 'Product successfully registered!' });
		} catch (error) {
			console.error(error);
			res.status(500).json({ message: 'Error registering product' });
		}
	}
}

export default ProductController;