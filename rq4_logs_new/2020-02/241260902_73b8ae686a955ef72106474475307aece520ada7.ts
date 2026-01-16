import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

const routes: Routes = [
  { path: '', redirectTo: 'login', pathMatch: 'full' },
  { path: 'login', loadChildren: './pages/login/login.module#LoginPageModule' },
  { path: 'home', loadChildren: './pages/home/home.module#HomePageModule' },
  { path: 'new-item', loadChildren: './pages/new-item/new-item.module#NewItemPageModule' },
  { path: 'update-item', loadChildren: './pages/update-item/update-item.module#UpdateItemPageModule' },
  { path: 'register', loadChildren: './pages/register/register.module#RegisterPageModule' },
  { path: 'staff-add', loadChildren: './pages/staff-add/staff-add.module#StaffAddPageModule' },
  {
    path: 'report-staff',
    loadChildren:
      './pages/report-staff/report-staff.module#ReportStaffPageModule'
  },
  {
    path: 'report-supplier',
    loadChildren:
      './pages/report-supplier/report-supplier.module#ReportSupplierPageModule'
  },
  {
    path: 'report-products',
    loadChildren:
      './pages/report-products/report-products.module#ReportProductsPageModule'
  },
  {
    path: 'report',
    loadChildren: './pages/report/report.module#ReportPageModule'
  },
  {
    path: 'report-revenue',
    loadChildren:
      './pages/report-revenue/report-revenue.module#ReportRevenuePageModule'
  },
  {
    path: 'report-category',
    loadChildren:
      './pages/report-category/report-category.module#ReportCategoryPageModule'
  },
  {
    path: 'overview',
    loadChildren: './pages/overview/overview.module#OverViewPageModule'
  },
  { path: 'tabs', loadChildren: './pages/tabs/tabs.module#TabsPageModule' },
  { path: 'sell', loadChildren: './pages/sell/sell.module#SellPageModule' },
  {
    path: 'return-info',
    loadChildren: './pages/return-info/return-info.module#ReturnInfoPageModule'
  },
  {
    path: 'sell-bill',
    loadChildren: './pages/sell-bill/sell-bill.module#SellBillPageModule'
  },
  {
    path: 'others',
    loadChildren: './pages/others/others.module#OthersPageModule'
  },
  {
    path: 'management',
    loadChildren: './pages/management/management.module#ManagementPageModule'
  },
  { path: 'scan', loadChildren: './pages/scan/scan.module#ScanPageModule' },
  {
    path: 'update-catalog',
    loadChildren:
      './pages/update-catalog/update-catalog.module#UpdateCatalogPageModule'
  },
  {
    path: 'categori',
    loadChildren: './pages/categori/categori.module#CategoriPageModule'
  },
  {
    path: 'product',
    loadChildren: './pages/product/product.module#ProductPageModule'
  },
  {
    path: 'product-list',
    loadChildren:
      './pages/product-list/product-list.module#ProductListPageModule'
  },
  {
    path: 'product-detail',
    loadChildren:
      './pages/product-detail/product-detail.module#ProductDetailPageModule'
  },
  {
    path: 'product-import',
    loadChildren:
      './pages/product-import/product-import.module#ProductImportPageModule'
  },
  {
    path: 'product-import-suppliers',
    loadChildren:
      './pages/product-import-suppliers/product-import-suppliers.module#ProductImportSuppliersPageModule'
  },
  {
    path: 'product-import-detail',
    loadChildren:
      './pages/product-import-detail/product-import-detail.module#ProductImportDetailPageModule'
  },
  {
    path: 'product-import-confirm',
    loadChildren:
      './pages/product-import-confirm/product-import-confirm.module#ProductImportConfirmPageModule'
  },
  {
    path: 'product-import-add-suppliers',
    loadChildren:
      './pages/product-import-add-suppliers/product-import-add-suppliers.module#ProductImportAddSuppliersPageModule'
  },
  {
    path: 'product-import-add-product',
    loadChildren:
      './pages/product-import-add-product/product-import-add-product.module#ProductImportAddProductPageModule'
  },
  { path: 'sell1', loadChildren: './pages/sell1/sell1.module#Sell1PageModule' },
  {
    path: 'add-products',
    loadChildren:
      './pages/add-products/add-products.module#AddProductsPageModule'
  },
  {
    path: 'product-detail2',
    loadChildren:
      './pages/product-detail2/product-detail2.module#ProductDetail2PageModule'
  },
  {
    path: 'product-import-cart',
    loadChildren:
      './pages/product-import-cart/product-import-cart.module#ProductImportCartPageModule'
  },
  {
    path: 'system-store-info',
    loadChildren:
      './pages/system-store-info/system-store-info.module#SystemStoreInfoPageModule'
  },
  {
    path: 'system-printer',
    loadChildren:
      './pages/system-printer/system-printer.module#SystemPrinterPageModule'
  },
  { path: 'system', loadChildren: './pages/system/system.module#SystemPageModule' },
  { path: 'return-cart', loadChildren: './pages/return-cart/return-cart.module#ReturnCartPageModule' },
  { path: 'sell-info', loadChildren: './pages/sell-info/sell-info.module#SellInfoPageModule' },
  { path: 'return-bill', loadChildren: './pages/return-bill/return-bill.module#ReturnBillPageModule' }



];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }