import { postCheckWatchlist } from 'backend/services/laravel-api/postCheckWatchlist';
import { getRemoveWatchlist } from 'backend/services/laravel-api/getRemoveWatchlist';
import { postAddWatchlist } from 'backend/services/laravel-api/postAddWatchlist';

export class postWatchlistController {
    async handle(data: string, token: string){
        
        const checkService = new postCheckWatchlist();

        /// checa se o usuario tem esse filme já favoritado
        return await checkService.execute(token, data);
    }

    async choice(have: boolean, data: string, token: string){
        if ( have ) {
            const removeWatchlist = new getRemoveWatchlist();
            return await removeWatchlist.execute(token, data);
        }
        else {
            const addWatchlist = new postAddWatchlist();
            return await addWatchlist.execute(token, data);
        }
    }

}
