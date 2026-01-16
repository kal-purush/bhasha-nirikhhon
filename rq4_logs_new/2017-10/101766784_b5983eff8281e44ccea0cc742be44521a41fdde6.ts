import { EventEmitter } from '@angular/core';
import { Http, Response, Request } from '@angular/http';
import 'rxjs/add/operator/toPromise';
import { FileUploaderOptions, FileUploader } from "./FileUploader.class";
export declare class ngfUploader extends FileUploader {
    Http: Http;
    ref: ngfUploader;
    refChange: EventEmitter<ngfUploader>;
    options: FileUploaderOptions;
    useNgHttp: any;
    constructor(Http: Http);
    ngOnInit(): void;
    ngOnChanges(changes: any): void;
    uploadFiles(files: File[]): Promise<any>;
    xhrOneByOne(files: File[]): Promise<any[]>;
    ngHttpFiles(formData: FormData): Promise<Response>;
    postRequest(config: Request): Promise<Response>;
}