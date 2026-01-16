/// <reference path="../../typings/angular2-meteor.d.ts" />

import {Component, View, NgIf, NgFor} from 'angular2/angular2';

import {MeteorComponent} from 'angular2-meteor';

import {RouterLink, RouteParams} from 'angular2/router';

import {[GNRT_UCCP]} from 'collections/[GNRT_UCCP]';

import {[GNRT_UCCS]Service, [GNRT_UCCS]Model} from 'client/[GNRT_LCCS]-list/[GNRT_UCCS]Service';

import {[GNRT_UCCS]Edit} from 'client/[GNRT_LCCS]-edit/[GNRT_UCCS]Edit';

import {Router} from 'angular2/router'


@Component({
    selector: '[GNRT_LCCS]-details'
})

@View({
    templateUrl: '/client/[GNRT_LCCS]-details/[GNRT_LCCS]-details.html',
    directives: [RouterLink, [GNRT_UCCS]Edit, NgIf, NgFor]
})

export class [GNRT_UCCS]Details extends MeteorComponent {

    private [GNRT_LCCS]Id:string;

    public [GNRT_LCCS]:[GNRT_UCCS]Model = new [GNRT_UCCS]Model();

    public [GNRT_LCCS]Status:string = '';

    private [GNRT_LCCS]Service:[GNRT_UCCS]Service;
    
    private router: Router;

    public constructor(params:RouteParams, [GNRT_UCCS]Service:[GNRT_UCCS]Service, Router:Router) {
        super();
        
        this.[GNRT_LCCS]Id = params.get('id');

        this.[GNRT_LCCS]Service = [GNRT_UCCS]Service;
        
        this.router = Router;
        
        this.autorun(()=> {
            this.subscribe('[GNRT_LCCS]', this.[GNRT_LCCS]Id, ()=> {
                this.[GNRT_LCCS] = <[GNRT_UCCS]Model>[GNRT_UCCP].findOne({_id: this.[GNRT_LCCS]Id});
                if (this.[GNRT_LCCS]){
                    this.[GNRT_LCCS]Status = this.[GNRT_LCCS]Service.[GNRT_LCCS]Status(this.[GNRT_LCCS].public);
                }
            }, true);
        }, true);
  
    }
    
    public deleteConfirmation(): void {
        var element:any = $('#modal3');
        element.openModal();
    }
    
    public delete[GNRT_UCCS](): void{
        var element:any = $('#modal3');
        this.[GNRT_LCCS]Service.delete[GNRT_UCCS](this.[GNRT_LCCS]Id)
        element.closeModal();
        this.router.navigate(['/[GNRT_UCCS]List']);
    }
    
    
}


    

