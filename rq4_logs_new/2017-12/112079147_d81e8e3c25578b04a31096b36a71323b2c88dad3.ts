import {Component, OnDestroy, OnInit} from '@angular/core';
import {ProductSelectorsIds} from '../../globals/product-selectors-ids';
import {LocalStorageService} from '../../../../frontend/services/local-storage.service';
import {AlertService} from '../../../../frontend/services/alert.service';
import {ProductRepositoryService} from '../../services/product-repository.service';
import {MatDialogRef, MatTableDataSource} from '@angular/material';
import {BaseComponent} from '../../../../frontend/components/base-component/base.component';
import {ComponentFactoryService} from '../../../../frontend/services/component-factory.service';
import {Product} from '../../models/product.model';
import {IconsNames} from '../../../../../globals/icons-names';
import {HeaderActionItem} from '../../../../frontend/models/header-action-item.model';
import {DialogResult} from '../../../../frontend/models/dialog-result.model';
import {BaseCollectionComponent} from '../../../../frontend/components/base-collection/base-collection.component';
import {ProductSearchQuery} from '../../models/product-search.model';
import {ProductSearchListItemComponent} from '../product-search-list-item/product-search-list-item.component';
import {IProductSearchTableData, ProductSearchTableData} from '../../models/product-search-table-data.model';
import {IProductSearchTableElement, ProductSearchTableElement} from '../../models/product-search-table-element.model';

@Component({
  selector: ProductSelectorsIds.ProductSearchDialogSelector,
  templateUrl: './product-search-dialog.component.html',
  styleUrls: ['./product-search-dialog.component.less']
})
export class ProductSearchDialogComponent extends BaseCollectionComponent<Product> implements OnInit, OnDestroy {

  tableData: ProductSearchTableData;
  searchQuery: string;

  constructor(public dialogRef: MatDialogRef<ProductSearchDialogComponent>,
              protected _productRepositoryService: ProductRepositoryService,
              protected _localStorageService: LocalStorageService,
              protected _productService: ProductRepositoryService,
              protected _factoryService: ComponentFactoryService<Product, BaseComponent<Product>>,
              private _alertService: AlertService) {
    super(_productRepositoryService, _factoryService);
  }

  ngOnInit() {
    this.createActions();
  }

  ngOnDestroy() {
    this.destroyActions();
    this.destroyChildren();
  }

  createActions() {
    this.actions.push(
      new HeaderActionItem(
        IconsNames.Cancel,
        'Cancel',
        undefined,
        () => {
          this.onCancelClick();
        }
      )
    );
  }

  search(): void {
    if (this.searchQuery) {
      this._productService.search(new ProductSearchQuery(this.searchQuery))
        .then(result => {
          this.model = result;
          // this.destroyChildren();
          // this.createComponents(result);
          this.modelToTable(this.model);
        });
    } else {
      // this.destroyChildren();
    }
  }

  destroyActions(): void {
    this.actions = [];
  }

  onCancelClick(): void {
    this.dialogRef.close(new DialogResult(false));
  }

  modelToTable(model: Product[]): void {
    if (model && model.length > 0) {
      const tableSource = [];
      const tableColumns = ['image', 'quantity', 'unit', 'name', 'calories', 'weight'];
      for (const item of model) {
        const tableElementSpec = <IProductSearchTableElement>{
          image: item.productInformation.imageUriThumb,
          quantity: +item.servingInformation.quantity,
          unit: item.servingInformation.unit,
          name: item.productInformation.productName,
          calories: +item.nutritionFacts.calories,
          weight: +item.servingInformation.weightGrams
        };

        tableSource.push(new ProductSearchTableElement(tableElementSpec));
      }
      const tableDataSpec = <IProductSearchTableData>{
        columns: tableColumns,
        source: new MatTableDataSource<ProductSearchTableElement>(tableSource)
      };
      this.tableData = new ProductSearchTableData(tableDataSpec);
    }
  }

  createComponents(model: Product[]): void {
    this._factoryService.setRootViewContainerRef(this.collectionItemsContainer);

    for (const item of model) {
      const newComponent = this.factoryService.addDynamicComponentWithExtendedModel({
        model: item,
      }, ProductSearchListItemComponent);
      this.components.push(newComponent);
    }
  }
}