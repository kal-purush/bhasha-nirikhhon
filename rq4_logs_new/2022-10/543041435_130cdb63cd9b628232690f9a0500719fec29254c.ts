import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Basket } from 'src/entity/Basket.entity';
import { ShoppingCartService } from 'src/shopping-cart/shopping-cart.service';
import { BasketController } from './basket.controller';
import { BasketService } from './basket.service';

@Module({
  imports: [TypeOrmModule.forFeature([Basket])],
  providers: [BasketService, ShoppingCartService],
  controllers: [BasketController]
})
export class BasketModule {}