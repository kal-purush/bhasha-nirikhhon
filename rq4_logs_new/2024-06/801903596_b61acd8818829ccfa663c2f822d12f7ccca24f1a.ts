import asyncHandler from 'express-async-handler';
import { Request, Response } from 'express';
import { CartService } from '../services/cart.service';
import { CustomSessionData } from '../interfaces/session.interface';
import { t } from 'i18next';

export class CartController {
  private readonly cartService: CartService;
  constructor(){
    this.cartService = new CartService();
  }
  
  public addToCart = asyncHandler(
    async (req: Request, res: Response) => {
      const user = (req.session as CustomSessionData).user;
      const productId = Number(req.body?.productId);
      if(user!==undefined){
        try {
          await this.cartService.addToCart(user, productId);
          res.status(200).send({ 
            success: true, 
            message: t('cart.add.success'), 
          });
        } catch (error) {
          res.status(500).send({ 
            success: false, 
            message: t('cart.add.fail'), 
          });
        }
      }else{
        res.redirect('/auth/login');
      }
    },
  );
}