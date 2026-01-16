import { EventEmitter,Injectable } from '@angular/core';
import {Http,Headers,Response} from '@angular/http';
import {Observable} from 'rxjs/Rx';

declare var xml2json: any;

@Injectable()
export class AppStoreViewService {
    public addComponentChanged = new EventEmitter<Object>();
    private header: Headers;
    private subscription:any;
    private baseUrl: string;
    public layout:any;
    constructor(private http : Http){
      this.baseUrl = "http://fe-z12ff.de.bosch.com:3088/getComponent";
        this.header = new Headers();
		this.header.append('Content-Type', 'application/xml');
    this.addComponentChanged = new EventEmitter<Object>();

    }

    public addComponent(componentContextMenu:ContextMenu){
      this.addComponentChanged.emit({layout:this.layout,clickdContexMenu:componentContextMenu});
    }

    public  getComponent() {
          return this.http.get(this.baseUrl,this.header)
          .map(this.extractData)
                  .catch(this.handleError);
                    
  }

  private extractData(res: Response) {
  return JSON.parse(xml2json(res.text(),'  ')).ArrayOfComponent;
  //return "";
}

private handleError (error: any) {
  // In a real world app, we might use a remote logging infrastructure
  // We'd also dig deeper into the error to get a better message
  let errMsg = (error.message) ? error.message :
    error.status ? `${error.status} - ${error.statusText}` : 'Server error';
  console.error(errMsg); // log to console instead
  return errMsg;
}
}