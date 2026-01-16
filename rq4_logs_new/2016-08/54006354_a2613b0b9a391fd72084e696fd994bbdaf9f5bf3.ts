import { Directive, Host, HostListener, Input } from "@angular/core";

import {CompleterData} from "../components/ng2-completer/services/completer-data";
import {CtrCompleter} from "./ctr-completer";


// keyboard events
const KEY_DW = 40;
const KEY_RT = 39;
const KEY_UP = 38;
const KEY_LF = 37;
const KEY_ES = 27;
const KEY_EN = 13;
const KEY_TAB = 9;

const MIN_SEARCH_LENGTH = 3;
const MAX_CHARS = 524288;  // the default max length per the html maxlength attribute
const PAUSE = 250;

@Directive({
    selector: "[ctr-input]",
})
export class CtrInput {
     @Input() public minSearchLength = MIN_SEARCH_LENGTH;
     @Input() public maxChars = MAX_CHARS;

    constructor(@Host() private completer: CtrCompleter) {}

    @HostListener("keyup", ["$event"]) public keyupHandler(event: any) {
        if (event.keyCode === KEY_LF || event.keyCode === KEY_RT) {
            // do nothing
            return;
        }

        if (event.keyCode === KEY_UP || event.keyCode === KEY_EN) {
            event.preventDefault();
        }
        else if (event.keyCode === KEY_DW) {
            event.preventDefault();
            // if (!this.showDropdown && this.searchStr && this.searchStr.length >= this.minSearchLength) {
            //     this.initResults();
            //     this.searching = true;
            //     this.searchTimerComplete(this.searchStr);
            // }
        }
        else if (event.keyCode === KEY_ES) {
            // this.clearResults();
        }
        else {
            // this._onChangeCallback(this.searchStr);
            // if (!this.searchStr) {
            //     this.showDropdown = false;
            //     return;
            // }
            // if (this.searchStr === "") {
            //     this.showDropdown = false;
            // }
            // else if (this.searchStr.length >= this.minSearchLength) {
            //     this.initResults();

            //     if (this.searchTimer) {
            //         clearTimeout(this.searchTimer);
            //     }

            //     this.searching = true;

            //     this.searchTimer = setTimeout(
            //         () => {
            //             this.searchTimerComplete(this.searchStr);
            //         },
            //         this.pause
            //     );
            // }
        }

    }
}