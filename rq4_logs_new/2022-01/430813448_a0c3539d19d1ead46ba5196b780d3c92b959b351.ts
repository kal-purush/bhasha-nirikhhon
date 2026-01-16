import { getRemoveFavorite } from './../../services/laravel-api/getRemoveFavorite';
import { postCheckFavorite } from "backend/services/laravel-api/postCheckFavorite";
import { postAddFavorite } from 'backend/services/laravel-api/postAddFavorite';

export class postFavoriteController {
    async handle(data: string, token: string){
        
        const checkService = new postCheckFavorite();
        let have = false;

        /// checa se o usuario tem esse filme já favoritado
        return await checkService.execute(token, data);
    }

    async choice(have: boolean, data: string, token: string){
        if ( have ) {
            const removeFavorite = new getRemoveFavorite();
            return await removeFavorite.execute(token, data);
        }
        else {
            const addFavorite = new postAddFavorite();
            return await addFavorite.execute(token, data);
        }
    }

}
