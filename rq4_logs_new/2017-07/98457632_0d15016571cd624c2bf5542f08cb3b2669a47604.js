import $ from 'jquery';
import _ from 'lodash';
import { FIREBASE_CONFIG, FIREBASE_CONFIG_V2  } from '../config';
import firebase from 'firebase';

/**
 * @fileOverview View handles updating the visual elements on the page
 */
export default class Reindex {
    /**
     * Construct the view and run initialization
     *
     */
    constructor($element) {
        // If view element isn't on the page
        if (!$element.length) {
            return;
        }

        this.$element = $element;
        this.itemsOrigin = {};
        this.itemsNew = {};

        firebase.initializeApp(FIREBASE_CONFIG);
        this._firebaseOrigin = firebase.database();
        //firebase.initializeApp(FIREBASE_CONFIG_V2);
        //this._firebaseNew = firebase.database();

        this.init();
    }

    init() {
        this.loadData();

        return this;
    }

    run() {
        this.loadData();
    }

    loadData() {
        const ref = this._firebaseOrigin.ref();
        ref.once("value", (snapshot) => {
            this.itemsOrigin = snapshot.val();
        }, (error) => {
            console.log("Firebase Get Error: " + error.code);
        })
        .then(() => {
            this.formatData();
        });
    }

    formatData() {
        _.forEach(this.itemsOrigin, (value, key) => {
            console.log(this.itemsOrigin.key);
            if (this.itemsNew[value.Name]) {
                this.itemsNew[value.Name].Id.push(key);
            } else {
                this.itemsNew[value.Name] = {
                    Id: [key],
                    Description: value.Description,
                    Rarity: value.Rarity,
                }
            }
        });
        this.$element.html(JSON.stringify(this.itemsNew));
    }
}