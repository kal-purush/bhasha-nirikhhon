module mpp.controller.keys {

    export function handle(ctrl: main.MainCtrl, e: JQueryEventObject): boolean {
        if (e.altKey || e.ctrlKey || e.shiftKey) {
            return false;
        }

        var kc = e.keyCode;
        //console.log(`cc=${kc}`);
        var ZERO = 48;
        var NINE = 57;
        var ZERO_NUMPAD = 96;
        var NINE_NUMPAD = 105;
        var numPadNum = kc >= ZERO_NUMPAD && kc <= NINE_NUMPAD;
        if (kc >= ZERO && kc <= NINE || numPadNum) {
            var numOff = numPadNum ? ZERO_NUMPAD : ZERO;
            var markerNumber = kc === ZERO || kc === ZERO_NUMPAD ? 10 : (kc - numOff);
            ctrl.seek(markerNumber);
            return true;
        } else if (kc === 32) { // space
            ctrl.playPause();
            return true;
        } else if (kc === 77 || kc === 65) { // m or a
            ctrl.addMarker();
            return true;
        } else if (kc === 13) { // return
            ctrl.seekToStart();
            return true;
        } else if (kc === 37 || kc === 39) { // left or right
            ctrl.backwardForward(kc === 37);
            return true;
        } else if (kc === 8) { // backspace
            ctrl.seekToActiveMarker();
            return true;
        } else if (kc === 38 || kc === 40) { // up or down
            ctrl.moveActiveMarker(kc === 38);
            return true;
        }

        return false;
    }

}