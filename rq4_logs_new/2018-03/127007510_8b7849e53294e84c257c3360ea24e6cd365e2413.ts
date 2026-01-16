import { Pipe, PipeTransform } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';

@Pipe({
  name: 'youtubeVideo'
})
export class YoutubeVideoPipe implements PipeTransform {

  constructor(private domSanitizer:DomSanitizer) {

  }

  transform(value:string): any {
    let youtubeUrl = "https://www.youtube.com/embed/";

    return this.domSanitizer.bypassSecurityTrustResourceUrl(youtubeUrl + value);
  }

}