import 'angular';
import 'angular-route';
import './home/home';

angular.module('app',['home','ngComponentRouter'])
    .config(function($locationProvider) {
        $locationProvider.html5Mode(true);
    })
    .value('$routerRootComponent', 'component');
    
class ComponentController {
    public hello:string;
    constructor(){
        console.log('constructor');
    }
    
    public $onInit(){
        console.log('on init');
        
        this.hello = 'toto';
        
    }
}



angular.module('app')
    .component('component',{
        controller: ComponentController,
        template:
        '<div>'+
        '{{$ctrl.hello}}'+
        '<a ng-link="[Home]">Home</a>'+
        '<a ng-link="[Page1]">Page1</a>'+        
        '</div>'+
        '<ng-outlet></ng-outlet>',
        $routeConfig:[
            {
                path:'/',
                name:'Home',
                component:'home',
                useAsDefault:true
            }
        ]
    });
    
/**
 * MainController
 */