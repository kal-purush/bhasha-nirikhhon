/**
 * Created on 2017-02-01.
 * @author: Gman Park
 */

import {Component, AfterViewInit} from "@angular/core";
import {Http} from "@angular/http";

declare var Swiper: any;

@Component({
    moduleId: module.id,
    selector: 'movie',
    templateUrl: './movie.component.html',
    styleUrls: ['./movie.component.css']
})

export class MovieComponent implements AfterViewInit {
    public static AppKey = 'f2dc1bac4738f37ff5d783ab52a512b5';

    ngAfterViewInit(): void {
        var mySwiper = new Swiper('.swiper-container', {
            // Optional parameters
            direction: 'vertical',
            loop: true,

            // If we need pagination
            pagination: '.swiper-pagination',

            // Navigation arrows
            nextButton: '.swiper-button-next',
            prevButton: '.swiper-button-prev'
        })
    }

    constructor(public http: Http) {
        this.http = http;
        console.log(this.http);
        let url = 'https://apis.daum.net/contents/movie?apikey=f2dc1bac4738f37ff5d783ab52a512b5&q=%EC%98%81%ED%99%94&output=json';

        this.http.get(url).subscribe((res)=>{
            debugger;
            console.log();
        })
            // .then(response => console.log(response.json().data))
            // .catch(this.handleError);
    }

    private handleError(error: any): Promise<any> {
        console.error('An error occurred', error); // for demo purposes only
        return Promise.reject(error.message || error);
    }
}