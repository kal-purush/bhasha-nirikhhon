import { FormsModule } from "@angular/forms";
import { CommonModule } from "@angular/common";
import { ReactiveFormsModule } from "@angular/forms";
import { Root } from "./root.component";
import { ModuleWithProviders, Injector } from "@angular/core";
import { RouterModule, Routes } from "@angular/router";

export function getAppModule() {
  return System.import(''+ (process.env.AOT ? '../../aot/src/client/app/app.module.ngfactory' : './app/app.module'))
   .then(mod => mod[(process.env.AOT ? 'AppModuleNgFactory' : 'AppModule')]);
}

const routes: Routes = [
    { path: "", loadChildren: getAppModule } 
];
const AppRouteModule: ModuleWithProviders = RouterModule.forRoot(routes);

const modules = [
    FormsModule, CommonModule, ReactiveFormsModule,
    AppRouteModule
];

const _declarations = [Root];
const _providers = [Injector];
const _imports = modules;

export function declarations(): Array<any> {
    return _declarations;
}
export function providers(): Array<any> {
    return _providers;
}
export function imports(): Array<any> {
    return _imports;
}