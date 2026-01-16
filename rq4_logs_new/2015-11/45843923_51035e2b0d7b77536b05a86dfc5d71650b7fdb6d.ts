import {global} from 'angular2/src/core/facade/lang';

import {PromiseCompleter, PromiseWrapper} from 'angular2/src/core/facade/promise';
import {TimerWrapper} from 'angular2/src/core/facade/async';

import {VisibleResult} from './visible-result';

/**
 * Animation Values class helps to check current animation state and could runs cancel or cleanup operations.
 */
export class AnimatingValues {
  private static animatingValues:WeakMap<HTMLElement, AnimatingValues> = new WeakMap<HTMLElement, AnimatingValues>();

  private completer:PromiseCompleter<VisibleResult> = PromiseWrapper.completer();

  private timer:any;

  constructor(private element:HTMLElement, private cleanupFunction:Function, private finishFunction:Function) {
    global.assert(AnimatingValues.animatingValues.get(this.element));
    AnimatingValues.animatingValues.set(this.element, this);
  }

  /**
   * Check does any animation happens on an element.
   */
  static isAnimating(element:HTMLElement):boolean {
    let values:AnimatingValues = AnimatingValues.animatingValues.get(element);
    return values != null;
  }

  /**
   * Cancel current animation on an element.
   */
  static cancelAnimation(element:HTMLElement):void {
    let values:AnimatingValues = AnimatingValues.animatingValues.get(element);
    global.assert(values);
    values.cancel();
  }

  /**
   * Run clean up operation later.
   */
  static scheduleCleanup(durationMS:number, element:HTMLElement, cleanupFunction:Function, finishFunction:Function):Promise<VisibleResult> {
    let value:AnimatingValues = new AnimatingValues(element, cleanupFunction, finishFunction);
    return value.start(durationMS);
  }

  /**
   * Run animation after specified amount of time. This method returns the Promise of animation operation.
   */
  private start(durationMS:number):Promise<VisibleResult> {
    global.assert(durationMS > 0);
    global.assert(this.timer);

    this.timer = TimerWrapper.setTimeout(() => {
		this.complete();
	}, durationMS);
    return this.completer.promise;
  }

  /**
   * Stop execution of an animation, clean up and inform all listeneres of Promise of animation operation that operation was VisibleResult.CANCELED.
   */
  private cancel():void {
    global.assert(this.timer);
    TimerWrapper.clearTimeout(this.timer); 
	this.timer = null;
    this.cleanup();
    this.completer.resolve(VisibleResult.CANCELED);
  }

  /**
   * Complete animation and inform all listeneres of Promise of animation operation that operation was VisibleResult.ANIMATED.
   */
  private complete():void {
    this.cleanup();
    this.finishFunction(this.element);
    this.completer.resolve(VisibleResult.ANIMATED);
  }

  /**
   * Clean up after canceletion or completion of animation operation.
   */
  private cleanup():void {
    global.assert(AnimatingValues.animatingValues.get(this.element));
    this.cleanupFunction(this.element);
    AnimatingValues.animatingValues.set(this.element, null);
  }
}