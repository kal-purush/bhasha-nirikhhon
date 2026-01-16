import {NgModule} from '@angular/core';
import {Routes, RouterModule} from '@angular/router';
import {MainPageComponent} from "./main-page/main-page.component";
import {NotFoundComponent} from "./not-found/not-found.component";

const routes:Routes = [
    {path: '', component: MainPageComponent},
    // {path: 'good/:id', component: GoodComponent},
    {path: '**', component: NotFoundComponent}
];

@NgModule({
    imports: [RouterModule.forRoot(routes)],
    exports: [RouterModule]
})
export class AppRoutingModule {
}

