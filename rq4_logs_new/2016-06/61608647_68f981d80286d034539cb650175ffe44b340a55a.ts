import { Injectable } from '@angular/core';
import { Http, Response, Headers, RequestOptions, RequestMethod } from '@angular/http';

@Injectable()
export class GuidedWorkoutService {
    private DashBoardSigninUrl = "https://dashboard.microsofthealth.com/Home/MobileRedirect";
    private DashBoardEndpointUrl = "https://dashboard.microsofthealth.com/workout/start?orderByField=DIFFICULTY&orderByAscending=true&filtersParam=%5B%5D&query=&pageSize=20";

    constructor(private http: Http) {

    }

    /*
        public CreateNewUserProfile() {
            let header = new Headers();
            header.append("Authorization", this.MSAToken);
                    
            let requestOptions = new RequestOptions({
                headers: header,
                method: RequestMethod.Get,
            });
            this.http.get(this.KDSEndPointAPIUrl, requestOptions).subscribe(response => {
                window.alert(response.headers.get("Authorization"));
            }, error => {
                window.alert(error);
            });
        }
    */

    // sign in into dashboard, mainly for getting the cookies to query the guided workouts
    public signInDashboard() {
        return new Promise((resolve, reject) => {
            var browserRef = window.open(this.DashBoardSigninUrl, "_blank", "location=no, closebuttoncaption=Done");
            browserRef.addEventListener("loadstart", (event) => {
                if (event.url == "https://dashboard.microsofthealth.com/") {
                    resolve();
                    browserRef.close();
                }
            })
        });
    }

    // get guided workouts
    public queryGuidedWorkout() {
        return new Promise((resolve, reject) => {
            this.http.get(this.DashBoardEndpointUrl).subscribe(Response => {

            }, error => {

            });
        });
    }
}