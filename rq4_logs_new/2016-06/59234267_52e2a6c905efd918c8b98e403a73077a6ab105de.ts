import {Component, OnInit, Input} from '@angular/core';
import {NavParams, ViewController} from "ionic-angular/index";
import {Channel} from "../../../domain/channel/Channel";

@Component({
    selector: 'vp-colorpicker',
    templateUrl: 'build/pages/controller/channel/colorpicker.component.html'
})
export class VpColorpickerComponent {
    @Input()
    channel:Channel;

    private visible = false;

    toggle() {
        console.log("...")
        this.visible = !this.visible;
    }

    isActive():boolean {
        return this.visible;
    }

    getColor():String {
        return "#" + this.componentToHex(this.channel.state.color.r) + this.componentToHex(this.channel.state.color.g) + this.componentToHex(this.channel.state.color.b);
    }

    private componentToHex(c) {
        var hex = c.toString(16);
        return hex.length == 1 ? "0" + hex : hex;
    }
}