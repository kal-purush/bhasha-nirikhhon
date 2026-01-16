import { CreateCategoryDto } from './dto/CreateCategoryDto';
import { CategoryService } from './category.service';
import { Controller, Body, Get, Post, Param } from '@nestjs/common';

@Controller('category')
export class CategoryController {
    constructor(private readonly categoryService: CategoryService) {}

    @Post()
    async create(@Body() createCategoryDto: CreateCategoryDto) {
        this.categoryService.create(createCategoryDto);
    }

    @Get(':substring/:options')
    async find(@Param() params) {
        console.log(params.substring, params.options);
        return this.categoryService.find(params.substring, params.options);
    }

    @Get(':id')
    async findById(@Param() params) {
        return this.categoryService.findById(params.id);
    }
}