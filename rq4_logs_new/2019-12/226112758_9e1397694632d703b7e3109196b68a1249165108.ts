/**
 * Created by Administrator on 2019/12/18.
 */
import {Component, OnInit} from "@angular/core";
import {RefreshableTab} from "../../tab/tab.component";
import {TableQueryParams, ResultVO, TableResultVO} from "../../../../core/model/result-vm";
import {ProductService} from "../../../../core/services/product.service";
import {ProductVO} from "../../../../core/model/product";
import {HttpErrorResponse} from "@angular/common/http";
import {NzMessageService} from "ng-zorro-antd";

@Component({
  selector: 'app-product-list',
  templateUrl: './product-list.component.html',
  styleUrls: ['./product-list.component.less']
})
export class ProductListComponent implements RefreshableTab, OnInit{

  totalPages : number = 1;
  pageIndex : number = 1;
  pageSize : number = 1;

  name : string;
  type : number;

  productList: ProductVO[];

  constructor(
    private product : ProductService,
    private message: NzMessageService,
  ){

  }
  refresh(): void {
  }

  ngOnInit(): void {
      this.search();
  }

  search() : void{
    const queryParams: TableQueryParams = {
      pageIndex : this.pageIndex,
      pageSize : this.pageSize,
      name : this.name,
      type : this.type
    }

    this.product.findAllByPage(queryParams)
      .subscribe((res: ResultVO<TableResultVO<ProductVO>>) =>{
        if(!res){
          return;
        }
        if(res.code !== 200){
          return;
        }

        const tableResult: TableResultVO<ProductVO> = res.data;
        this.pageIndex = tableResult.pageIndex;
        this.pageIndex = tableResult.pageIndex;
        this.pageSize = tableResult.pageSize;
        this.productList = tableResult.result;
    }, (error: HttpErrorResponse) => {
        this.message.error(error.message);
      });
  }

}