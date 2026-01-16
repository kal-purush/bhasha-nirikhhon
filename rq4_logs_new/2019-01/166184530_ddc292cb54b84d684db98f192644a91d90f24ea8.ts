import {Controller, Get, HttpException, HttpStatus, Param} from '@nestjs/common';
import {ApiUseTags, ApiImplicitParam} from '@nestjs/swagger';
import {ArticleService} from './article.service';

@Controller('articles')
@ApiUseTags('Articles')
export class ArticleController {

    constructor(private readonly articleService: ArticleService) {}

    @Get()
    async getAll() {
        return await this.articleService.getAll();
    }

    @Get(':id')
    @ApiImplicitParam({name: 'id'})
    async getOne(@Param('id') id) {
        const article = await this.articleService.getOne(id);
        if (article) return article;
        throw new HttpException('Article not found !', HttpStatus.NOT_FOUND);
    }

}