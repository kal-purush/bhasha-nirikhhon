export const DEFAULT_EVENT_OPTIONS = { bubbles: true, cancelable: true };
const ALL_KEYBOARD_EVENT_TYPES = Object.freeze([
  'keydown',
  'keypress',
  'keyup',
]);
export type KeyboardEventType = 'keydown' | 'keypress' | 'keyup';
export function isKeyboardEventType(s: string): s is KeyboardEventType {
  return ALL_KEYBOARD_EVENT_TYPES.indexOf(s) > -1;
}

const ALL_MOUSE_EVENT_TYPES = [
  'click',
  'mousedown',
  'mouseup',
  'dblclick',
  'mouseenter',
  'mouseleave',
  'mousemove',
  'mouseout',
  'mouseover',
];
export type MouseEventType =
  | 'click'
  | 'mousedown'
  | 'mouseup'
  | 'dblclick'
  | 'mouseenter'
  | 'mouseleave'
  | 'mousemove'
  | 'mouseout'
  | 'mouseover';
export function isMouseEventType(s: string): s is MouseEventType {
  return ALL_MOUSE_EVENT_TYPES.indexOf(s) > -1;
}

const ALL_FILE_SELECTION_EVENT_TYPES = ['change'];
export type FileSelectionEventType = 'change';
export function isFileSelectionEventType(
  s: string
): s is FileSelectionEventType {
  return ALL_FILE_SELECTION_EVENT_TYPES.indexOf(s) > -1;
}

export type EventType =
  | KeyboardEventType
  | MouseEventType
  | FileSelectionEventType
  | 'abort'
  | 'afterprint'
  | 'animationend'
  | 'animationiteration'
  | 'animationstart'
  | 'appinstalled'
  | 'audioprocess'
  | 'audioend '
  | 'audiostart '
  | 'beforeprint'
  | 'beforeunload'
  | 'beginEvent'
  | 'blocked	'
  | 'blur'
  | 'boundary '
  | 'cached'
  | 'canplay'
  | 'canplaythrough'
  | 'chargingchange'
  | 'chargingtimechange'
  | 'checking'
  | 'close'
  | 'complete'
  | 'compositionend'
  | 'compositionstart'
  | 'compositionupdate'
  | 'contextmenu'
  | 'copy'
  | 'cut'
  | 'devicechange'
  | 'devicelight'
  | 'devicemotion'
  | 'deviceorientation'
  | 'deviceproximity'
  | 'dischargingtimechange'
  | 'downloading'
  | 'drag'
  | 'dragend'
  | 'dragenter'
  | 'dragleave'
  | 'dragover'
  | 'dragstart'
  | 'drop'
  | 'durationchange'
  | 'emptied'
  | 'end'
  | 'ended'
  | 'endEvent'
  | 'error'
  | 'focus'
  | 'focusin'
  | 'focusout'
  | 'fullscreenchange'
  | 'fullscreenerror'
  | 'gamepadconnected'
  | 'gamepaddisconnected'
  | 'gotpointercapture'
  | 'hashchange'
  | 'lostpointercapture'
  | 'input'
  | 'invalid'
  | 'languagechange '
  | 'levelchange'
  | 'load'
  | 'loadeddata'
  | 'loadedmetadata'
  | 'loadend'
  | 'loadstart'
  | 'mark '
  | 'message'
  | 'messageerror'
  | 'nomatch'
  | 'notificationclick'
  | 'noupdate'
  | 'obsolete'
  | 'offline'
  | 'online'
  | 'open'
  | 'orientationchange'
  | 'pagehide'
  | 'pageshow'
  | 'paste'
  | 'pause'
  | 'pointercancel'
  | 'pointerdown'
  | 'pointerenter'
  | 'pointerleave'
  | 'pointerlockchange'
  | 'pointerlockerror'
  | 'pointermove'
  | 'pointerout'
  | 'pointerover'
  | 'pointerup'
  | 'play'
  | 'playing'
  | 'popstate'
  | 'progress'
  | 'push'
  | 'pushsubscriptionchange'
  | 'ratechange'
  | 'readystatechange'
  | 'repeatEvent'
  | 'reset'
  | 'resize'
  | 'resourcetimingbufferfull'
  | 'result '
  | 'resume '
  | 'scroll'
  | 'seeked'
  | 'seeking'
  | 'select'
  | 'selectstart'
  | 'selectionchange'
  | 'show'
  | 'soundend '
  | 'soundstart '
  | 'speechend'
  | 'speechstart'
  | 'stalled'
  | 'start'
  | 'storage'
  | 'submit'
  | 'success'
  | 'suspend'
  | 'SVGAbort'
  | 'SVGError'
  | 'SVGLoad'
  | 'SVGResize'
  | 'SVGScroll'
  | 'SVGUnload'
  | 'SVGZoom'
  | 'timeout'
  | 'timeupdate'
  | 'touchcancel'
  | 'touchend'
  | 'touchmove'
  | 'touchstart'
  | 'transitionend'
  | 'unload'
  | 'updateready'
  | 'upgradeneeded	'
  | 'userproximity'
  | 'voiceschanged'
  | 'versionchange'
  | 'visibilitychange'
  | 'volumechange'
  | 'waiting'
  | 'wheel';