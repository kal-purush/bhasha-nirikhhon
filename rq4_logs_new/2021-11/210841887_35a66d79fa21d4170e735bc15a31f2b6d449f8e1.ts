import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class PDFService {

  pdfMake: any;

  constructor() { }

  private async loadPdfMake() {
    // we use lazy-loading to download the library only when it is needed
    if (!this.pdfMake) {
      const pdfMakeModule = await import('pdfmake/build/pdfmake');
      const pdfFontsModule = await import('pdfmake/build/vfs_fonts');
      this.pdfMake = pdfMakeModule.default;
      this.pdfMake.vfs = pdfFontsModule.default.pdfMake.vfs;
    }
  }

  public async generatePdf(data) {
    await this.loadPdfMake();

    this.pdfMake
      .createPdf(data)
      .open();
  }
}