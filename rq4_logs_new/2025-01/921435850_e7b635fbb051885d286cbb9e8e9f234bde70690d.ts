import { Module } from '@nestjs/common';
import { ProductsService } from './products.service';
import { ProductsController } from './products.controller';
import { MongooseModule } from '@nestjs/mongoose';
import { products, productsSchema } from '../schemas/products.schema';

@Module({
  imports: [
            MongooseModule.forFeature([
              {
                name: products.name,
                schema: productsSchema,
              },
            ]),
  ],

  controllers: [ProductsController],
  providers: [ProductsService],
})
export class ProductsModule {}

