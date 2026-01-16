import { Component, OnInit, Inject, ViewChild } from '@angular/core';
import { MatDialog, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { ProductoService } from './../producto.service';
@Component({
  selector: 'app-producto-new',
  templateUrl: './producto-new.component.html',
  styleUrls: ['./producto-new.component.css']
})
export class ProductoNewComponent implements OnInit {

  descripcion: string = "";
  stock: string;
  @ViewChild("fileInput") fileInput;
  constructor(private _service: ProductoService, public dialogRef: MatDialogRef<ProductoNewComponent>,
    @Inject(MAT_DIALOG_DATA) public data: any) { }

  ngOnInit() {
  }

  guardarProducto() {
    const fi = this.fileInput.nativeElement;
    this._service.guardarProducto(fi, this.descripcion,this.stock)
      .subscribe(() => {
        this.dialogRef.close();
      });
  }
}