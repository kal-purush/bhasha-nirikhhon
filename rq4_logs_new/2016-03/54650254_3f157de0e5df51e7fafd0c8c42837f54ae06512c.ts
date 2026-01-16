import {register} from 'platypus';
import BaseViewControl from '../base/base.vc';

export default class GreetingViewControl extends BaseViewControl {
    templateString: string = require('./greeting.vc.html');

    context: any = {
        name: ''
    };

    
    navigatedTo(parameters: any, query: any){
        //params nametogreet will have the value that was in the route parameter :nameto greet()
        this.context.name = parameters.nameToGreet;
    
    }
        
}


register.viewControl('greeting-vc', GreetingViewControl);