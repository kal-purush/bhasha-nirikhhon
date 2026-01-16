import { NgModule } from "@angular/core";
import { Routes, RouterModule } from "@angular/router";
import { MainPageComponent } from "./main-page/main-page.component";

const routes: Routes = [
    {
        path: "",
        redirectTo: "main",
        pathMatch: "full"
    },
    {
        path: "main",
        component: MainPageComponent
    },
    {
        path: "upload",
        loadChildren: "./upload/upload.module#UploadModule"
    }
];

@NgModule({
    imports: [RouterModule.forRoot(routes)],
    exports: [RouterModule]
})
export class AppRoutingModule {}