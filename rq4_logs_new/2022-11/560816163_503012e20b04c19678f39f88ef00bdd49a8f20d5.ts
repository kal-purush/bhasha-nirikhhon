import { HTTP_INTERCEPTORS } from "@angular/common/http";
import { NgModule } from "@angular/core";
import { AuthInterceptorService } from "./auth/auth-interceptor.service";
import { AuthService } from "./auth/auth.service";
import { RecipeService } from "./recipes/recipe.service";
import { RecipesResolverService } from "./recipes/recipes-resolver.service";
import { DataStorageService } from "./shared/data-storage.service";
import { ShoppingListService } from "./shopping-list/shopping-list.service";


@NgModule({
    providers: [
        ShoppingListService,
        DataStorageService,
        RecipeService,
        RecipesResolverService,
        AuthService,
        {provide: HTTP_INTERCEPTORS, useClass: AuthInterceptorService, multi: true}
    ]
})
export class CoreModule{

}