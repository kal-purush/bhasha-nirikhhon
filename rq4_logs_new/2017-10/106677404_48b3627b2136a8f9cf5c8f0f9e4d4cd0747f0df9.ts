import { Injectable } from '@angular/core';
import { Http, Headers, RequestOptions, URLSearchParams, Response } from '@angular/http';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/toPromise';
// import { ToolService } from './tool-service';
// import { Md5 } from 'ts-md5/dist/md5';

/*
  Generated class for the ApiService provider.

  See https://angular.io/docs/ts/latest/guide/dependency-injection.html
  for more info on providers and Angular 2 DI.
*/
// 正式服务器和账号
const API_HOST: string = "http://14.33.133.79";//"http://14.33.133.79";
const API_KEY:  string = "3bde7fa81ca14a5d9f829bcae9c72c67";

// 测试账号和测试服务器
// const API_HOST: string = "http://10.19.0.121:3000/api/v1";
// const API_KEY:  string = "1e3bb5a6e93148d7a6aa20ce181c1c46";

export const APP_VERSION = '1.0';

@Injectable()
export class ApiService {

  constructor(public http: Http) {
    // console.log('Hello ApiService Provider');
  }

  // 处理GET请求
  get(uri, params) {
    let url = API_HOST + '/' + uri;

    // 获取时间戳
    let i = new Date().getTime();

    // 组装参数
    let searchParams = new URLSearchParams();
    
    // // 设置安全参数
    // searchParams.set('i', i.toString());
    // searchParams.set('ak', this.generateAccessKey(i));

    // 合并传进来的参数
    for (let param in params) {
      searchParams.set(param, params[param]);
    }

    // this.tool.showLoading('加载中...');
    // 参数签名
    // searchParams.set('sign', ApiService.signParams(params));

    return this.http.get(url, new RequestOptions({ search: searchParams }))
      .toPromise()
      .then(this.handleSuccess)
      .catch(this.handleError);
  } // end get 

  // 处理POST请求
  post(uri, params) {
    let url = API_HOST + '/' + uri;

    // // 参数签名
    // params.sign = ApiService.signParams(params);

    // // 组装参数
    // let i  = new Date().getTime();
    // let ak = this.generateAccessKey(i);

    // params.i  = i;
    // params.ak = ak; 
    // this.tool.showLoading();

    // 封装请求
    let headers = new Headers({ 'Content-Type': 'application/json' });
    let requestOptions = new RequestOptions({ headers: headers });
    return this.http.post(url, JSON.stringify(params), requestOptions)
      .toPromise()
      .then(this.handleSuccess)
      .catch(this.handleError);
  } // end post

  // 上传文件
  upload(uri, body: FormData) {
      let url = API_HOST + '/' + uri;

      // 组装参数
      let i  = new Date().getTime();
      let ak = this.generateAccessKey(i);

      body.append('i', i.toString());
      body.append('ak', ak);

      // let headers = new Headers({'Content-Type': 'multipart/form-data'});
      return this.http.post(url, body, null)
      .toPromise()
      .then(this.handleSuccess)
      .catch(this.handleError);
  }

  // FormData提交
  post2(uri, body: FormData) {
    let url = API_HOST + '/' + uri;

      // 组装参数
      let i  = new Date().getTime();
      let ak = this.generateAccessKey(i);

      body.append('i', i.toString());
      body.append('ak', ak);

      // let headers = new Headers({'Content-Type': 'multipart/form-data'});
      return this.http.post(url, body, null)
      .toPromise()
      .then(this.handleSuccess)
      .catch(this.handleError);
  }

  // 生成MD5
  private generateAccessKey(i): string {
    return null;//Md5.hashStr(API_KEY + i.toString(), false).toString();
  } // end generate access key

  // 处理请求成功的回调
  private handleSuccess(resp: Response) {
    let body = resp.json();
    console.log(`result: ${body}`);
    // this.tool.hideLoading();
    return body;
    // if (body.code == 0) {
    //   if (body.total) {
    //     return { total: body.total, data: body.data };
    //   }
    //   return body.data || {};
    // } else {
    //   return Promise.reject(body.message);
    // }
  } // end handle success

  static signParams(params: any): string {
    if (!params) return null;
    // console.log(params);
    let signStr = '';
    let keys = Object.keys(params).sort();
    // console.log(`keys:${keys}`);
    if ( keys.length == 0 ) return null;

    keys.forEach(key => {
      let value = params[key];
      // console.log(value + ':' + JSON.stringify(value));
      signStr += value + ':';
    })

    signStr += API_KEY;

    return null;//Md5.hashStr(signStr, false).toString();
  }

  // 处理请求失败的回调
  private handleError(error: Response | any) {
    let errMsg: string;
    if ( error instanceof Response ) {
      const body = error.json() || '';
      const err  = body.error || JSON.stringify(body);
      errMsg = `${error.status} - ${error.statusText || ''} ${err}`;
    } else {
      errMsg = error.message ? error.message : error.toString();
    }

    // this.tool.hideLoading();

    return Promise.reject(errMsg);
  } // end handle error 

}