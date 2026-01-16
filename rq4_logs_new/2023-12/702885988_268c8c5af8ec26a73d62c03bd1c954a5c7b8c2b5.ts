import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import Swal from 'sweetalert2';
import { HoaDonService } from '../../service/hoadon/hoadon.service';
import { IHoaDon } from '../../service/hoadon/hoadon.module';
@Component({
  selector: 'app-payment-success',
  templateUrl: './payment-success.component.html',
  styleUrls: ['./payment-success.component.scss']
})
export class PaymentSuccessComponent implements OnInit {

  idHoaDon !: number;
  latestHoaDon: IHoaDon | undefined;

  IHoaDon: { [tab: string]: IHoaDon[] } = {};

  constructor(private route: ActivatedRoute,
    private hoaDonService: HoaDonService,
  ) { }

  ngOnInit(): void {
    this.fetchLatestHoaDon();

    Swal.fire({
      title: "Print Invoice?",
      text: "Do you want to print the invoice?",
      icon: "question",
      showCancelButton: true,
      confirmButtonColor: "#3085d6",
      cancelButtonColor: "#d33",
      confirmButtonText: "Yes, print!"
    }).then((result) => {
      if (result.isConfirmed) {
        this.exportPDF();
        Swal.fire({
          title: "Success!",
          text: "Printed invoice.",
          icon: "success"
        });
      }
    });
  }

  private fetchLatestHoaDon(): void {
    this.hoaDonService.getLatestHoaDonWithTrangThai1().subscribe(
      (hoaDon: IHoaDon) => {
        this.latestHoaDon = hoaDon;
        console.log("invice new " + this.latestHoaDon.id);
      },
      (error) => {
        console.error('Error fetching latest HoaDon', error);
      }
    );
  }

  exportPDF(): void {
    if (this.latestHoaDon?.id !== undefined) {
      const id = this.latestHoaDon.id;
      console.log("id new invoice " + id);
      this.hoaDonService.exportPdf(id).subscribe(
        (data) => {
          this.downloadFile(data);
        },
        (error) => {
          console.error('Error exporting PDF', error);
        }
      );
    }
  }

  private downloadFile(data: Blob): void {
    const blob = new Blob([data], { type: 'application/pdf' });
    const link = document.createElement('a');
    link.href = window.URL.createObjectURL(blob);
    link.download = 'hoadon.pdf';
    link.click();
  }
}

