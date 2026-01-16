/// <reference path="typings/tsd.d.ts" />

interface KnockoutSwipeBindingHandler extends KnockoutBindingHandler {
	insideUpdate: boolean;
    insideCallback: boolean;
    firstTime: boolean;
    logging: boolean;
    defaultDuration: number;
}

interface KnockoutBindingHandlers {
	swipe: KnockoutSwipeBindingHandler;
	swipeForeach: KnockoutBindingHandler;
}