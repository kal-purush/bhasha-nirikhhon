import { NgModule } from "@angular/core";
import { Routes, RouterModule, ProductComponent } from "@angular/router";

import { AdminComponent } from "./pages";

const routes: Routes = [
  {
    path: "admin",
    component: AdminComponent,
    children: [{ path: "product", component: ProductComponent }]
  }
];

// /app/src/admin/pages/admin.component.ts

// /app/src/admin/pages/rights/rights.component.ts

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class AdminRoutingModule {}