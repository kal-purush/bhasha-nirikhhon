import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { UrlPrefixService } from './url-prefix.service';
import { HttpClient, } from '@angular/common/http';

@Injectable()
export class ImageService{

  constructor(protected http: HttpClient, protected urlPrefix: UrlPrefixService) { }
  
  protected asMultiplesObjects: boolean = false;
  protected structureObject: any;
  protected urlCurrentObject: string = '';
  protected urlJson: string = '';
  protected maxPages: number = 0;
  
  /** GET image from server */
  getImage(angle: number, maxWidth: number, minHeight:number): Observable<Blob> {
    let query = "";
    query += '?angle='+angle;
    if(maxWidth > 0)
      query += '&max_width='+maxWidth;
    if(minHeight > 0)
      query += '&max_height='+minHeight;
    return this.http.get(this.urlPrefix.imageRender + this.urlCurrentObject+ query, {responseType: 'blob'});
  }

  /** GET metadata from the document */
  getMetadataImage(): Observable<Object[]> {
    return this.http.get<Object[]>(this.urlPrefix.metadataImage + this.urlCurrentObject);
  }

  /** Download the image */
  downloadImage(){
    return this.http.get(this.urlPrefix.downloadImage + this.urlCurrentObject, {responseType: 'blob'});
  }
  
  /** GET metadata from json */
  getMetadataJSON(): Observable<Object> {
    return this.http.get<Object[]>(this.urlPrefix.metadataJSON + this.urlJson);
  }

  /** GET metadata from json */
  getPhysicalJSON(): Observable<Object> {
    return this.http.get<Object[]>(this.urlPrefix.physicalJSON + this.urlJson);
  }

  /** SET url document from the document for the service */
  setUrlObject(url: string){
    this.urlCurrentObject = url;
  }

  /** GET url document from the document for the service */
  getUrlObject(): string{
    return this.urlCurrentObject;
  }

  /** SET url json of document from the document for the service */
  setUrlJSON(url: string){
    this.urlJson = url;
  }

  /** GET url json of document from the document for the service */
  getUrlJSON(): string{
    return this.urlJson;
  }

  /** SET max page */
  setMaxPage(pages: number){
    this.maxPages = pages;
  }

  /** GET max page */
  getMaxPage(): number{
    return this.maxPages;
  }

  /** SET structure */
  setStructureObject(struc: any) {
    this.structureObject = struc;
  }

  /** SET structure */
  getStructureObject() {
    return this.structureObject;
  }

  /** SET info about of multiple structure */
  setAsMultipleOnjects(isMultiple: boolean){
    this.asMultiplesObjects = isMultiple;
  }

  /** GET info about struture object */
  getAsMultipleOnjects(): boolean {
    return this.asMultiplesObjects;
  }


}