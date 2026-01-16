import {Component, View, NgFor} from 'angular2/angular2';

// @Component({
// 	selector: "player", // element with this attributes will get the directive
// 	templateUrl: 'templates/player.html',
// 	hostListeners: {
// 		'mouseover': 'onMouseEnter()', // automatically call functions depending on specific event on targeted element
// 		'mouseleave': 'onMouseLeave()'
// 	}
// })
@Component({
	selector: 'player',
	bind: {
        'src': 'src'
    }
})
@View({
	templateUrl: 'templates/player.html',
    directives: [NgFor]
})

export class Player {
	src: String;
    isPlaying: Boolean = false;
    element: HTMLElement;

    constructor() {
        this.element = document.querySelector("player video");
        console.log(this.element);
    }

    playPause() {
        this.isPlaying = !this.isPlaying;

        // We can't access this file in Chrome debug tools, we need to debug with debugger statements here
        debugger;

        if (this.isPlaying) this.element.play();
        else this.element.pause();
    }
}