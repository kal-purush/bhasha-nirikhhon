import { Component, OnInit } from "@angular/core";
import { RouterExtensions } from "nativescript-angular/router";
import { SnackBar } from "nativescript-snackbar";
const applicationSettings = require('tns-core-modules/application-settings');
import {FirebaseService} from '../../services/firebase.service';
import {User} from '../../models/user.model';
// import { EventData } from "tns-core-modules/ui/page/page";
@Component({
    moduleId: module.id,
    selector: "rr-mainComponent",
    templateUrl: "mainComponent.html",
})
export class MainComponent implements OnInit {
    isAuthenticating = false;
    public user: User;

    public constructor(private router: RouterExtensions, private firebaseService: FirebaseService) {

    }

    public ngOnInit() {
        if(applicationSettings.getBoolean("authenticated", false)) {
            this.router.navigate(["/home"], { clearHistory: true });
        }
    }

    public logout() {
        applicationSettings.remove("authenticated");
        this.router.navigate(["/login"], { clearHistory: true });
    }
    
    public onNewGroup(args) {
      alert("Tapped!");
  }

}
