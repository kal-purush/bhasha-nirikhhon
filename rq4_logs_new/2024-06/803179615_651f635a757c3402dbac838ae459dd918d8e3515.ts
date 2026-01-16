import {Request, ResponseToolkit} from '@hapi/hapi';
import {ArticleCategoriesService} from '../../services/postgresql/ArticleCategoriesService';
import {ArticleCategoryPayload} from '../../types/article-category-payload';

export class ArticleCategoriesHandler {
  private service: ArticleCategoriesService;
  constructor(service: ArticleCategoriesService) {
    this.service = service;
  }

  postArticleCategory = async (request: Request, h: ResponseToolkit) => {
    const payload = request.payload as ArticleCategoryPayload;
    const {id, name} = await this.service.addArticleCategory(payload);

    return h
      .response({
        status: 'success',
        message: 'New category added successfully',
        data: {
          id,
          name,
        },
      })
      .code(201);
  };

  getAllArticleCategories = async (request: Request, h: ResponseToolkit) => {
    const articleCategories = await this.service.retrieveArticleCategories();

    return h
      .response({
        status: 'success',
        data: {
          articleCategories,
        },
      })
      .code(200);
  };

  getCategoryById = async (request: Request, h: ResponseToolkit) => {
    const {id} = request.params;
    const category = await this.service.retrieveArticleCategoryById(id);

    return h.response({
      status: 'success',
      data: {
        id: category.id,
        name: category.name,
      },
    });
  };

  putArticleCategoryById = async (request: Request, h: ResponseToolkit) => {
    const {id} = request.params;
    const {name} = request.payload as ArticleCategoryPayload;
    const {name: newName} = await this.service.updateArticleCategoryById(
      id,
      name
    );

    return h.response({
      status: 'success',
      message: 'Successfully update name of the article category',
      data: {
        name: newName,
      },
    });
  };

  deleteArticleCategoryById = async (request: Request, h: ResponseToolkit) => {
    const {id} = request.params;

    const {id: idDeleted} = await this.service.deleteArticleCategoryById(id);

    return h.response({
      status: 'success',
      message: `Article category with id of ${idDeleted} deleted successfully`,
    });
  };
}