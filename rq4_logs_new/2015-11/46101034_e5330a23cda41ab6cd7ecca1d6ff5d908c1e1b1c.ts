import {bootstrap, Component} from 'angular2/angular2';
import { SelectBox } from './components/SelectBox';
import { OptionsService } from './services/OptionsService';

@Component({
    selector: 'my-app',
    directives: [SelectBox],
    template: `
        <h1>My First Angular 2 App</h1>
        <select-box></select-box>    
    `
})
class AppComponent { }

bootstrap(AppComponent, [OptionsService]);