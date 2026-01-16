import { Component, Inject, OnInit } from "@angular/core";
import { VoucherSevice } from "../service/voucher.service";
import { MAT_DIALOG_DATA,MatDialog } from "@angular/material/dialog";
// import * as moment from 'moment';
// import { utcToZonedTime } from 'date-fns-tz';


@Component({
  selector :'app-voucher-dialog',
  templateUrl:'./voucher-Dialog.component.html',
  styleUrls: ['./voucher-Dialog.component.scss'],
})

export class VoucherDialogComponent implements OnInit{
voucher:any ={};
type: any;

  ngOnInit(): void {

  }

  constructor(
    @Inject(MAT_DIALOG_DATA) public data: any,
    private voucherService: VoucherSevice,
    private dialog: MatDialog,

  ) {

    this.type = data.type;
    this.voucher = data.voucher;

  }

addVoucher(){
  console.log('time chua set:', this.voucher.thoiGianKetThuc);


  this.voucher.thoiGianKetThuc = new Date(this.voucher.thoiGianKetThuc).toISOString();
  console.log('tim da set:',this.voucher.thoiGianKetThuc);
this.voucherService.createVoucher(this.voucher).then((res) => {
  console.log('data created', res.content);
  if (res) {
    this.dialog.closeAll();
  }
});
}
updateVoucher(){
  this.voucherService.updateVoucher(this.voucher, this.voucher.id).then((res) => {
    if (res) {
      this.dialog.closeAll();
    }
    console.log('data updated', res.content);
  });

}
deleteVoucher() {
  this.voucherService.deleteVoucher(this.voucher.id);
  this.dialog.closeAll();
}




}


