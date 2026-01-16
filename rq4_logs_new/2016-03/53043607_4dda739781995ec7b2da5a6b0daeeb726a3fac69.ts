import {Component} from 'angular2/core';
import {Input} from 'angular2/core';
import {ComponentRef} from 'angular2/core';
import {AlertConfig} from '../../app/alert/alert.config';
@Component({
    selector : 'alert',
    template: `<div (click)="Dismiss()" class='alert-custom alert alert-{{style}}'>{{MessageContent}}
                    <img *ngIf='imagePath' src={{imagePath}}/>
                </div>`,
   styles: [`
               .alert-custom {
                   cursor:pointer;
               }
               .alert-custom img {
                   width:30px;
                   heigth:30px;
               }
            `]
})
export class AlertComponent{   
    
   @Input() style: string;
   @Input() imagePath: string;
   @Input() public MessageContent : string;  
   private componentRef : ComponentRef;
   constructor(private alertConfig: AlertConfig){
        this.MessageContent = alertConfig.message || "";
        this.style = alertConfig.style || "success";
        this.imagePath = alertConfig.imagePath || null;       
    }
    
    contentRef(componentRef: ComponentRef) {
        this.componentRef = componentRef;
    }
    
    Dismiss(){
        this.componentRef.dispose();
    }
 
}