import {Component} from 'angular2/core';
import {ResizerDirective} from './attribute-directives/resizer.directive';

@Component({
  selector: 'my-app',
  template: `<div id="topnav" >Top navbar </div>
             <div id="sidebar">
              <h3>Side navbar</h3>
             </div>

             <div id="sidebar-resizer" jmResizer [resizerWidth]=6
                    [resizerLeft]="'sidebar'" [resizerRight]="'content'"></div>

             <div id="content">
              <div id="top-content">Top Content</div>
              <div id="bottom-content">Bottom Content</div>
             </div>
            `,
  styleUrls: ['app/app.component.css'],
  directives: [ResizerDirective]
})

export class AppComponent {

}