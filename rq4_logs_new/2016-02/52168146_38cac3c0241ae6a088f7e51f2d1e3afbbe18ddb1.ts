import {bootstrap} from 'angular2/platform/browser';
import {Component, enableProdMode, Injectable, OnInit} from 'angular2/core';
import {Http, Headers, HTTP_PROVIDERS, URLSearchParams, Response} from 'angular2/http';
import {Observable} from 'rxjs/Rx';
import 'rxjs/add/operator/map';  // lib is large, add only the map operator as result
import {FORM_DIRECTIVES} from "angular2/common";
import {RequestOptions} from "angular2/http";


@Injectable()
class NodeApi {

    constructor(private http: Http) {
        console.log('NodeApi Constructor')
    }

    get() {
        const endpoint = 'http://127.0.0.1:8081/calls';
        // http.get returns an Observable from RxJS lib and map is an operator in this lib
        // the RxJS lib implements an asynchronous observable pattern
        var response = this.http.get(endpoint).map((res: Response) => res.json().response).catch(this.handleError);
        console.log(' response ' + response)
        return response
    }

    post(value) {
        console.log('in the post? ' + JSON.stringify(value))
        const endpoint = 'https://127.0.0.1:8081/calls';
        const headers = new Headers({'Content-Type': 'application/json'});
        let options = new RequestOptions({headers: headers});
        var body = JSON.stringify({"value": value});
        return this.http.post(endpoint, body, options)
            .map((res: Response) => res.json()).subscribe()
    }

    private handleError (error: Response) {
        // in a real world app, we may send the server to some remote logging infrastructure
        // instead of just logging it to the console
        console.error(error);
        return Observable.throw(error.json().error || 'Server error');
    }
}


@Component({
    selector: 'app',
    template:
        `<div class="container">
        <div class="jumbotron text-center">
            <h3><span class="fa fa-thumbs-o-up"></span> {{title}}</h3>
        <!-- we use ngModel to allow two-way data-binding to the call_value defined in App-->
            <div>
                <!-- we use a simple form here (template form) to issue a call to our getter for the 8081 server -->
                <form f="getForm" (ngSubmit)="doGet()">
                    <button type="submit" class="btn btn-warning btn-lg">GET</button>
                    {{call_value | async}}  <!--This pipe accepts a promise or observable as input, updating the view with the appropriate value(s) when the promise is resolved or observable emits a new value. -->
                </form>
            </div>
            <div>
                <form f="postForm" (ngSubmit)="doPost()">
                    <button type="submit" class="btn btn-warning btn-lg">POST</button>
                    <input [(ngModel)]="call_post" placeholder="0">
                </form>
            </div>
        </div>
        </div>

`,
    directives: [FORM_DIRECTIVES],
    providers: [HTTP_PROVIDERS, NodeApi],
})



class App implements OnInit {
    public title = 'Simple Server'
    // this variable will be populated by a node server running in the background
    public call_value;
    public call_post;

    constructor(private nodeApi: NodeApi) {
        this.call_post=0
        console.log('FYI: console message appear in your browser console')
    }

    public doGet() {
        this.call_value = this.nodeApi.get();
    }

    public doPost() {
        console.log('call_post ' + this.call_post)
        this.nodeApi.post({"value": this.call_post})
    }

    ngOnInit() {
        console.log('init called')
        this.call_value = this.nodeApi.get();
    }
}

//enableProdMode();
bootstrap(App)
    .catch(err => console.error(err));
