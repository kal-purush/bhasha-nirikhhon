import { Pipe, PipeTransform } from '@angular/core';
import { DomSanitizer, SafeHtml, SafeStyle, SafeScript, SafeUrl, SafeResourceUrl } from '@angular/platform-browser';

/**
 * Generated class for the ContenidoSeguroPipe pipe.
 *
 * See https://angular.io/api/core/Pipe for more info on Angular Pipes.
 */
@Pipe({
  name: 'contenidoSeguro',
})
export class ContenidoSeguroPipe implements PipeTransform {
  
  constructor(protected sanitizer: DomSanitizer) { }

  public transform(value: any, type: string): SafeHtml | SafeStyle | SafeScript | SafeUrl | SafeResourceUrl {
    var result: any = {}; 
    console.log("ContenidoSeguroPipe");
    console.log(JSON.stringify(value));

    if(!value)
    {
      value="";
    }

    switch (type) {
      case 'html': result = this.sanitizer.bypassSecurityTrustHtml(value); break;
      case 'style': result = this.sanitizer.bypassSecurityTrustStyle(value); break;
      case 'script': result = this.sanitizer.bypassSecurityTrustScript(value); break;
      case 'url': result = this.sanitizer.bypassSecurityTrustUrl(value); break;
      case 'resourceUrl': result = this.sanitizer.bypassSecurityTrustResourceUrl(value); break;
      default: throw new Error(`Invalid safe type specified: ${type}`);
    }
 
    return result;
  }
  
}