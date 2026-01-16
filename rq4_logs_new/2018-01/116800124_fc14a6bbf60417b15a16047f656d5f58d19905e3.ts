import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'gender'
})
export class genderPipe implements PipeTransform {

  /**
   * 性別代碼轉換
   * 
   * @param {string} gender 
   * @returns {*} 
   * @memberof genderPipe
   */
  transform(gender: string): any {
    if ('F'.indexOf(gender) > -1) {
      return '女';
    } else if ('M'.indexOf(gender) > -1) {
      return '男';
    } else {
      return '';
    }
  }

}