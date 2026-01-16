import { CreateArticleDto } from './dto/CreateArticleDto';
import { ArticleService } from './article.service';
import { Post, Body, Controller, Get } from '@nestjs/common';

@Controller('article')
export class ArticleController {
    constructor(private readonly articleService: ArticleService) {}

    @Post()
    async create(@Body() createArticleDto: CreateArticleDto) {
        this.articleService.create(createArticleDto);
    }

    @Get()
    async findAll() {
        return this.articleService.findAll()
    }
}