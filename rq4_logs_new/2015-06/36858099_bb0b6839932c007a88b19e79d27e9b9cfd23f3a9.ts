/// <reference path="events.ts" />

module GrowthMessage {
    export class App extends GrowthMessage.Events {
        id:string;
        el:any;
        config = new GrowthMessage.Config();
        dialog = new GrowthMessage.Dialog();
        image = new GrowthMessage.Image();
        userAgent = new GrowthMessage.UserAgent();

        constructor(options:{id:string}) {
            super();
            if(!this.userAgent.isViewable()) return;
            this.id = options.id;
            this.render();
            this.setStyles();
            this.bindEvents();

            // For Debug
            window.onhashchange = ()=>{
                location.reload(false);
            };
            this.config.load('/sample/json/' + location.hash.slice(1) + '.json');
        }
        render() {
            var el = document.createElement('div');
            el.className = 'growthmessage';
            document.body.appendChild(el);
            this.el = document.body.getElementsByClassName(el.className)[0];
        }
        setStyles() {
            var styles = GrowthMessage.module.require('styles.css');
            var el:any = document.createElement('style');
            el.type = 'text/css';
            el.innerHTML = styles;
            document.getElementsByTagName('head')[0].appendChild(el);
        }
        bindEvents() {
            this.on('hook', 'open');
            this.config.on('set', 'loadImages', this);
            this.image.on('load', 'open', this);
        }
        loadImages() {
            this.image.load(this.config.get());
        }
        open() {
            this.dialog.open(this.config.get());
        }
    }
}