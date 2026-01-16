import {NgModule} from '@angular/core';
import {RouterModule, Routes} from '@angular/router';
import {FormComponent} from "./form/form.component";
import {PreviewComponent} from "./preview/preview.component";


const routes: Routes = [

        {
          path: 'form',
          component: FormComponent,
        },  {
          path: 'preview',
          component: PreviewComponent,
        },

];

@NgModule({
    imports: [RouterModule.forRoot(routes)],
    exports: [RouterModule],

})
export class AppRoutingModule {
}


