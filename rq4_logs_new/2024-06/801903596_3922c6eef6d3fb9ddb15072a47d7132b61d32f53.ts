import asyncHandler from 'express-async-handler';
import {Request, Response} from 'express';
import { ProductService } from '../../services/product.service';
import { CustomSessionData } from '../../interfaces/session.interface';
import { plainToClass } from 'class-transformer';
import { ProductPageOptions } from '../../commons/dtos/productPageOptions.dto';
import { validate } from 'class-validator';
import { formatCurrency, formatDate, handleError } from '../../commons/utils';
import { AdminNavBar } from '../../constants/admin';
import { ProductEntity } from '../../entities/product.entity';
import { Error } from '../../constants';

export class AdminProductController {
  private readonly productService: ProductService;
  constructor() {
    this.productService = new ProductService();
  }

  public getAdminProductPage = asyncHandler(
    async (req: Request, res: Response) => {
      const admin = (req.session as CustomSessionData).admin;
      const pageOptions = plainToClass(ProductPageOptions, req.query);
      const rawErrors = await validate(pageOptions);
      const categories = await this.productService.getAllCategories();
      if(rawErrors.length > 0) {
        const errors = handleError(rawErrors, req, res);
        return res.render('admin/product/list', {
          admin,
          currentSite: AdminNavBar.PRODUCT,
          categories,
          errors,
          query: pageOptions,
          formatDate,
          formatCurrency,
        });
      }
      const productPage = await this.productService.getProductPage(pageOptions);
      return res.render('admin/product/list', {
        admin,
        currentSite: AdminNavBar.PRODUCT,
        products: productPage.data,
        categories,
        meta: productPage.meta,
        query: pageOptions,
        formatDate,
        formatCurrency,
      });
    },
  );

  public getAdminProductDetail = asyncHandler(
    async (req: Request, res: Response) => {
      const admin = (req.session as CustomSessionData).admin;
      const product = await this.getProductFromIdParam(req, res);
      if(product === null) {
        req.flash('error', Error.PRODUCT_NOT_FOUND);
        res.redirect('/admin/product');
      }
      return res.render('admin/product/detail', {
        admin, 
        currentSite: AdminNavBar.PRODUCT,
        product, 
        formatDate,
        formatCurrency,
      });
    },
  );

  private getProductFromIdParam = async (
    req: Request,
    res: Response,
  ): Promise<ProductEntity | null> => {
    const id = parseInt(req.params.id);
    if (isNaN(id)) {
      req.flash('error', 'error.invalidParameter');
      res.redirect('/admin/product');
    }
    return await this.productService.getProductDetail(id);
  };
}