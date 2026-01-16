import { HammerGestureConfig, HAMMER_GESTURE_CONFIG } from '@angular/platform-browser';

import * as Hammer from 'hammerjs';

export class HammerConfig extends HammerGestureConfig {
    overrides = <any>{
        'pinch': { enable: false },
        'rotate': { enable: false },
        'doubletap': { enable: false }
    };

    buildHammer(element: HTMLElement) {
        return new Hammer(element, {
            inputClass: Hammer.TouchInput,
            touchAction: 'pan-y'  // If using horizontal gestures - http://hammerjs.github.io/touch-action/
        });
    }
}