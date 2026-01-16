import $ from './libs/jquery.module.js';

export class Background {
    static node;

    static SetImage(url) {
        this.node.css('backgroundImage', `url(${url})`);
    }
    
    static init() {
        this.node = $('#--root');
    }
}

$(() => {
    Background.init();
})