import {Component} from 'angular2/core';
import {Tabs, Tab} from './tabs.component';

@Component({
    selector: 'my-app',
    directives: [Tabs, Tab],
    template:`
    <tabs [crossFade]=crossfade>
      <tab tabTitle="Tab Title 1 ">
        Here is the content of Tab title 1
      </tab>
      <tab tabTitle="Tab Title 2">
        Here is the content of Tab title 2
      </tab>
    </tabs>`
     
})
export class AppComponent {
  crossfade = true;
 }