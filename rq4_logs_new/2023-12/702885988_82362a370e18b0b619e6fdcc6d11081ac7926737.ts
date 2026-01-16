import { Component, Inject, OnInit } from '@angular/core';
import { ProductDetailService } from '../../services/product.service';
import { AngularFireStorage } from "@angular/fire/compat/storage"
import { MAT_DIALOG_DATA, MatDialog } from '@angular/material/dialog';
import { FormBuilder,Validators } from '@angular/forms';



@Component({
  selector: 'app-dialog-product-detail',
  templateUrl: './dialog-product-detail.component.html',
  styleUrls: ['./dialog-product-detail.component.scss']
})
export class dialogProductDetailComponent implements OnInit {
  productDetail: any ={};
  product: any[] = [];
  size: any[] = [];
  color: any[] = [];
  shoeMaterial: any[] = [];
  shoeSoleMaterial: any[] = [];

  validUrls: string[] = []; // Mảng để lưu đường dẫn hình ảnh

  type :any;
selectedSanPhamId: number | null =null;
selectedKichCoId : number | null =null;
selectedMauSacId : number | null =null;
selectedChatLieuGiayId : number | null =null;
selectedChatLieuDegiayId : number | null =null;
productDetailFrom :any ;

  ngOnInit(): void {
    if (this.data.productDetail && this.data.productDetail.sanPham && this.data.productDetail.mauSac && this.data.productDetail.kichCo
      && this.data.productDetail.chatLieuGiay && this.data.productDetail.chatLieuDeGiay) {
      // Thiết lập giá trị mặc định cho các trường select
      this.selectedSanPhamId = this.data.productDetail.sanPham.id;
      this.selectedKichCoId = this.data.productDetail.kichCo.id;
      this.selectedMauSacId = this.data.productDetail.mauSac.id;
      this.selectedChatLieuGiayId = this.data.productDetail.chatLieuGiay.id;
      this.selectedChatLieuDegiayId = this.data.productDetail.chatLieuDeGiay.id;

    }

  }

  constructor(
    @Inject(MAT_DIALOG_DATA) public data :any,
    private productDetailService: ProductDetailService,
    private fireStorage: AngularFireStorage,
    private dialog :MatDialog,
    private fb : FormBuilder,

  ) {

    this.shoeMaterial = data.shoeMaterials;
    this.size = data.sizes;
    this.color = data.colors;
    this.shoeSoleMaterial = data.shoeSoleMaterials;
    this.product = data.products;
    this.type = data.type;
    this.productDetail = data.productDetail;
    // console.log("messge "+this.sanPham)
    this.productDetailFrom = this.fb.group({
      soLuong: ['', Validators.required],
      giaBan: ['', Validators.required],
      sanPham: [null, Validators.required],
      kichCo: [null, Validators.required],
      mauSac: [null, Validators.required],
      chatLieuGiay: [null, Validators.required],
      chatLieuDeGiay: [null, Validators.required],
      trangThai: [1, Validators.required]
    });


  }
  // load anh
  async onFileChange(event: any) {
    const files = event.target.files;
    console.log('files-log', files);

    if (files && files.length > 0) {
      for (let i = 0; i < files.length; i++) {
        const file = files[i];
        const path = `images/${file.name}`;
        const uploadTask = await this.fireStorage.upload(path, file);
        const url = await uploadTask.ref.getDownloadURL();
        this.validUrls.push(url);
        console.log(`Uploaded file ${i}: ${this.validUrls}`);
      }
    }
  }

  addProduct(): void {
    // console.log('Valid URLs in addProduct:', this.validUrls);
    // Gán giá trị vào this.product.anhChinh
    const soLuongValue = this.productDetailFrom.get('soLuong').value;
    const giaBanValue = this.productDetailFrom.get('giaBan').value;
    const sanPhamId = this.productDetailFrom.get('sanPham').value;
    const kichCoId = this.productDetailFrom.get('kichCo').value;
    const mauSacId = this.productDetailFrom.get('mauSac').value;
    const chatLieuGiayId = this.productDetailFrom.get('chatLieuGiay').value;
    const chatLieuDeGiayId = this.productDetailFrom.get('chatLieuDeGiay').value;

    const trangThaiValue = this.productDetailFrom.get('trangThai').value;

    // Gán giá trị vào this.product
    this.productDetail = {
      soLuong: soLuongValue,
      giaBan: giaBanValue,
      sanPham: sanPhamId,
      kichCo: kichCoId,
      mauSac: mauSacId,
      chatLieuGiay:chatLieuGiayId,
      chatLieuDeGiay :chatLieuDeGiayId,
      trangThai: trangThaiValue,
    };

    this.productDetail.anhSanPhams = this.validUrls
  // console.log('Product Image URLs:', this.product.anhChinh);
    if (this.productDetailFrom.valid) {
        this.productDetailService.createProduct(this.productDetail).then(res => {
          console.log('Data created', res.content);
          if (res) {
            this.dialog.closeAll();
          }
        });
      } else {
        console.error('Image URLs are null or empty. Product not added.');
      }
    }

  }

  // updateProduct() {
  //   // const selectedBrand = this.brand.find(brand => brand.id === this.selectedBrandId);
  //   // const selectedOrigin = this.origin.find(origin => origin.id === this.selectedOriginId);
  //   // const selectedCategory = this.categories.find(category => category.id === this.selectedCategoryId);

  //   if (selectedBrand && selectedOrigin && selectedCategory) {
  //     this.product.thuongHieu = selectedBrand.id;
  //     this.product.xuatXu = selectedOrigin.id;
  //     this.product.danhMuc = selectedCategory.id;

  //     // Sử dụng đường dẫn ảnh mới nếu có
  //     // if (this.uploadedUrl) {
  //     // this.product.anhChinh = this.uploadedUrl;
  //     this.product.anhChinh = this.uploadedUrl || this.product.anhChinh;
  //     // }

  //     this.productService.updateProduct(this.product, this.product.id).then(res => {
  //       console.log('data updated', res.content);
  //       if (res) {
  //         this.dialog.closeAll();
  //       }
  //     });
  //   }
  // }









  // saveProductDetail() {
  //   this.productDetail.anhSanPhams = this.validUrls
  //   this.productDetailService.createProduct(this.productDetail).then(
  //     (data) => {
  //       console.log("data " + data);
  //         this.dialog.closeAll();
  //     },
  //     (error) => console.log(error)
  //   );
  // }



  // onSubmit() {
  //   console.log(this.newProduct);
  //   this.saveProductDetail();
  // }
