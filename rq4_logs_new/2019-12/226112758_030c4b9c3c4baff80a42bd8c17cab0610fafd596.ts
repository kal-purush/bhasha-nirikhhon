import { Pipe, PipeTransform } from "@angular/core";

@Pipe({
  name: 'processing_order_status'
})
export class ProcessingOrderStatusPipe implements PipeTransform {

  transform(value: number, ...args: any[]): any {
    switch (value) {
      case 1: return '已完成';
      default: return '已完成';
    }
  }

}

@Pipe({
  name: 'processing_order_status_color'
})
export class ProcessingOrderStatusColorPipe implements PipeTransform {

  transform(value: number, ...args: any[]): any {
    switch (value) {
      case 1: return 'green';
      default: return 'green';
    }
  }

}