import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { SnakeNamingStrategy } from 'typeorm-snake-naming-strategy';
import { Cart } from './entities/cart.entity';
import { CartItem } from './entities/cart-item.entity';
import { Product } from './entities/product.entity';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.PG_HOST,
      port: +process.env.PG_PORT,
      username: process.env.PG_USERNAME,
      password: process.env.PG_PASSWORD,
      database: process.env.PG_DATABASE,
      entities: ['dist/database/entities/*.entity{.ts,.js}'],
      /**
       * Flag to show all generated sql queries on each interaction with DB.
       * Should be omitted for production production.
       */
      logging: false,
      /**
       * This naming strategy will map field_name from database to fieldName inside application.
       */
      namingStrategy: new SnakeNamingStrategy(),
    }),
    TypeOrmModule.forFeature([Cart, CartItem, Product]),
  ],
  exports: [TypeOrmModule],
})
export class DatabaseModule {}