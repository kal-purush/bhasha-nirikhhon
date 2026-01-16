import {Component} from "@angular/core";
import {NavController, Alert} from 'ionic-angular';
import {LocalNotifications} from 'ionic-native';

export class DoseNotifications {

    constructor(private navController: NavController) {
        LocalNotifications.on("click", (notification, state) => {
            let alert = Alert.create({
                title: "Dose Amigos Medication Notification",
                subTitle: "You currently have pending medications.",
                buttons: ["OK"]
            });
            this.navController.present(alert);
        });
    }

    /**
     *  Method for scheduling a daily local notification.
     *
     *  @startDate javascript long representing the time to take a medication.
     */
    public schedule(startDate) {
        var d = new Date(startDate);

        LocalNotifications.schedule({
            title: "Dose Amigos Reminder",
            text: "Time to take your medication!",
            firstAt: new Date(d.getTime()),
            sound: null
        });
    }

}