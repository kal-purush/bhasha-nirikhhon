import { Body, Controller, Delete, Get, Param, Patch, Post, Query, UseGuards } from '@nestjs/common';
import { ProductService } from './product.service';
import { AuthGuard } from 'src/auth/auth.guard';
import { CreateProductDto } from './models/create-product.dto';
import { Product } from '@prisma/client';
import { UpdateProductDto } from './models/update-product.dto';

// @UseGuards(AuthGuard)
@Controller('products')
export class ProductController {
    constructor(private productService: ProductService) { }

    @Get()
    getAll(@Query('page') page: number = 1) {
        return this.productService.paginate(page);
    }

    @Post()
    async create(@Body() body: CreateProductDto): Promise<Product> {

        return this.productService.create({
            data: {
                title: body.title,
                description: body.description,
                image: body.image,
                price: body.price,
                userId: body.userId
            }
        });
    }

    @Get(':id')
    async get(@Param('id') id: number) {
        return this.productService.findOne({ where: { id: +id } })
    }

    @Patch(':id')
    async update(
        @Param('id') id: number,
        @Body() body: UpdateProductDto
    ) {
        return this.productService.update(+id, body);
    }

    @Delete(':id')
    async remove(@Param('id') id: number) {
        return this.productService.remove(+id);
    }
}