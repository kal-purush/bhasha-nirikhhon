import { CategoryService } from './../../services/category.service';
import asyncHandler from 'express-async-handler';
import {Request, Response} from 'express';
import { CustomSessionData } from '../../interfaces/session.interface';
import { plainToClass } from 'class-transformer';
import { validate } from 'class-validator';
import { handleError } from '../../commons/utils';
import { AdminNavBar } from '../../constants/admin';
import { t } from 'i18next';
import { CategoryPageOptions } from '../../commons/dtos/categoryPageOption.dto';
import { CreateCategoryDto } from '../../commons/dtos/createCategory.dto';
import { CategoryEntity } from '../../entities/category.entity';
import { UpdateCategoryDto } from '../../commons/dtos/updateCategory.dto';
import { Error } from '../../constants';

export class AdminCategoryController {
  private readonly categoryService: CategoryService;
  constructor() {
    this.categoryService = new CategoryService();
  }

  public getAdminCategoryPage = asyncHandler(
    async (req: Request, res: Response) => {
      const admin = (req.session as CustomSessionData).admin;
      const pageOptions = plainToClass(CategoryPageOptions, req.query);
      const rawErrors = await validate(pageOptions);
      if(rawErrors.length > 0) {
        const errors = handleError(rawErrors, req, res);
        return res.render('admin/category/list', {
          admin,
          currentSite: AdminNavBar.CATEGORY,
          errors,
          query: pageOptions,
        });
      }
      const categoryPage = 
      await this.categoryService.getCategoryPage(pageOptions);
      return res.render('admin/category/list', {
        admin,
        currentSite: AdminNavBar.CATEGORY,
        categories: categoryPage.data,
        meta: categoryPage.meta,
        query: pageOptions,
      });
    },
  );

  public createCategoryPost = asyncHandler(
    async (req: Request, res: Response) => {
      const createOption = plainToClass(CreateCategoryDto, req.body);
      const rawErrors = await validate(createOption);
      if(rawErrors.length > 0) {
        const errors = handleError(rawErrors, req, res);
        res.status(400).send({
          success: false,
          errors,
        });
      } else {
        try {
          const newCategory = await this.categoryService.createCategory(createOption);
          if(newCategory instanceof CategoryEntity) {
            res.status(201).send({
              success:true,
              message: t('category.create.success'),
            });
          }
        } catch (error) {
          res.status(400).send({
            success: false,
            error,
          });
        }
      }
    },
  );

  public updateCategory = asyncHandler(
    async (req: Request, res: Response) => {
      const category = await this.getCategoryFromIdParam(req, res);
      if(!category) {
        res.status(400).send({
          success: false,
          message: Error.CATEGORY_NOT_FOUND,
        });
      } else {
        const updateOption = plainToClass(CreateCategoryDto, req.body);
        const rawErrors = await validate(updateOption);
        if(rawErrors.length > 0) {
          const errors = handleError(rawErrors, req, res);
          res.status(400).send({
            success: false,
            errors,
          });
        } else {
          try {
            const updatedCategory = 
            await this.categoryService.updateCategory(category,updateOption);
            if(updatedCategory instanceof CategoryEntity) {
              res.status(200).send({
                success:true,
                message: t('category.update.success'),
              });
            }
          } catch (error) {
            res.status(400).send({
              success: false,
              error,
            });
          }
        }
      }
    },
  );

  public deleteCategory = asyncHandler(
    async (req: Request, res: Response) => {
      const category = await this.getCategoryFromIdParam(req, res);
      if(category === null) {
        res.status(400).send({
          success: false,
          message: t(Error.CATEGORY_NOT_FOUND),
        });
      } else {
        try {
          await this.categoryService.deleteCategory(category.id);
          res.status(200).send({
            success:true,
            message: t('store.delete.success'),
          });
        } catch (error) {
          res.status(400).send({
            success: false,
            error,
          });
        }
      }
    },
  );

  private getCategoryFromIdParam = async (
    req: Request,
    res: Response,
  ): Promise<CategoryEntity | null> => {
    const id = parseInt(req.params.id);
    if (isNaN(id)) {
      req.flash('error', 'error.invalidParameter');
      res.redirect('/admin/category');
    }
    return await this.categoryService.findOneById(id);
  };
}