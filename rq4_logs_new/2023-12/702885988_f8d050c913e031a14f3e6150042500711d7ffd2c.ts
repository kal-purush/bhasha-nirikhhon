import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { ICart } from '../service/cart.module';
import { CartService } from '../service/cart.service';
import { ToastrService } from 'ngx-toastr';

@Component({
  selector: 'app-cart',
  templateUrl: './cart.component.html',
  styleUrls: ['./cart.component.scss']
})
export class CartComponent implements OnInit {
  products!: any[];
  subTotal: number = 21000;
  cart!: ICart;
  listCart: any = [];
  items: any;
  home: any;
  checkedAll: any;
  clickCount = 0;
  checked: Set<number> = new Set();
  checkedItems: any = [];
  tongTien = 0;
  quantityInCart = 0;
  params: any = [];
  quantity = 0;
  updateQuantitys: any = [];


  constructor(
    private notificationService: ToastrService,
    private cartService: CartService,
    private router: Router
  ) {

  }
  ngOnInit(): void {
    this.items = [{ label: 'Giỏ Hàng' }];

    this.home = { icon: '', label: 'Trang chủ', routerLink: '/' };

    this.products = [{}]

    this.params.listIdGhct = this.checkedItems;
    Object.keys(this.params).forEach(key => {
      let value = this.params[key]
      if (value === null || value === undefined || value === '') {
        delete this.params[key];
      } else if (Array.isArray(value)) {
        if (value.length > 0) {
          this.params[key] = value.join(',');
        } else {
          delete this.params[key];
        }
      }
    });

    this.cartService.getAll().then((c) => {
      this.cart = c;
      this.listCart = c;
      c.forEach((key: any) => {
        this.quantityInCart++;
      })
    })



  }


  isChecked(itemId: number): boolean {

    // Kiểm tra xem itemId có trong Set không
    return this.checked.has(itemId);
  }

  toggleChecked(itemId: number): void {
    // Đảo ngược trạng thái của itemId trong Set
    if (this.checked.has(itemId)) {
      this.checkedItems = [];
      this.checked.delete(itemId);
      this.tongTien = 0;
    } else {
      this.checked.add(itemId);
      this.listCart.forEach((key: any) => {
        this.checkedItems.push(key.id);
      })
      this.tongTien = 0;
      this.cartService.getAll().then((c) => {
        this.cart = c;
        this.listCart = c;
        c.forEach((key: any) => {
          this.tongTien += (key.soLuong * key.giaBan);
        })
      })
    }

    // Kiểm tra xem checkedAll có chia hết cho 2 không, nếu chia hết thì bỏ chọn tất cả
    if (this.checkedAll % 2 === 0) {
      this.checked.clear();
      this.tongTien = 0;

    }

  }
  onCheckboxChange(event: any, itemId: number) {
    if (event.target.checked) {
      // Nếu checkbox được chọn, thêm giá trị vào mảng
      this.checkedItems.push(itemId);
      this.params.listIdGhct = this.checkedItems;
      console.log(this.params.listIdGhct);

      if (this.params.listIdGhct === undefined) {
        this.tongTien = 0;
      } else {
        this.tongTien = 0;
        this.cartService.findById(this.params).then((c) => {
          c.forEach((key: any) => {
            this.tongTien = this.tongTien + (key.soLuong * key.giaBan);
          })
        })

      }
    } else {
      // Nếu checkbox bị hủy chọn, loại bỏ giá trị khỏi mảng
      this.checkedItems = this.checkedItems.filter((id: number) => id !== itemId);

      this.params.listIdGhct = this.checkedItems;
      console.log(this.params.listIdGhct);

      if (this.params.listIdGhct.length === 0) {
        this.tongTien = 0;
      } else {
        this.tongTien = 0;
        this.cartService.findById(this.params).then((c) => {
          c.forEach((key: any) => {
            this.tongTien = this.tongTien + (key.soLuong * key.giaBan);
          })
        })

      }
    }
  }

  deleteCart(id: number) {
      this.params.listIdGhct = id;
      Object.keys(this.params).forEach(key => {
        let value = this.params[key]
        if (value === null || value === undefined || value === '') {
          delete this.params[key];
        } else if (Array.isArray(value)) {
          if (value.length > 0) {
            this.params[key] = value.join(',');
          } else {
            delete this.params[key];
          }
        }
      });

      this.cartService.deleteAllCart(this.params);
      this.notificationService.success("Xóa sản phẩm thành công");

      setTimeout(() => {
        window.location.reload();
      }, 1000);
    

  }

  updateQuantity(event:any, id: number) {
    this.params.listIdGhct = id;

    this.cartService.findById(this.params).then((c) => {
      c.forEach((key: any) => {
        this.cart = key;
      })
    })

 
    if(event.target.value === ''){
      this.notificationService.error("Vui lòng nhập số lượng");
    }else if (event.target.value <= 0) {
      this.notificationService.error("Số lượng phải lớn hơn 0");
    }else if(event.target.value > this.cart.chiTietSanPham.soLuong){
      this.notificationService.error("Số lượng phải nhỏ hơn "+this.cart.chiTietSanPham.soLuong);
    }else{
      this.updateQuantitys.id = id;   
      this.updateQuantitys.quantity = event.target.value;
      this.cartService.updateQuantity(this.updateQuantitys);
      this.notificationService.success('Cập nhật số lượng sản phẩm '+this.cart.chiTietSanPham.ma+' thành công');
      setTimeout(() => {
        window.location.reload();
      }, 1000);
    }
  }

  checkout() {
    if(this.checkedItems.length === 0){
      this.notificationService.error("Vui lòng chọn sản phẩm thanh toán");
    }
    // this.router.navigate(['/checkout'])
  }
}