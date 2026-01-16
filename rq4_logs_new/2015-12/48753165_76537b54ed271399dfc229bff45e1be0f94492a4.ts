import {
    Component,
    DynamicComponentLoader,
    Injector,
    ElementRef,
    Query,
    ViewQuery,
    QueryList,
} from 'angular2/core';
import {ChildComponent} from "./child.component";

@Component({
    selector: 'my-app',
    template: `
    <button (click)="loadComponent()">Load</button>
    <div #container attr.class="loadTarget" style="border: 2px solid blue">
    </div>
    <div #realContainer style="border: 2px solid lime">
    </div>
    `
})
export class AppComponent {
    private realContainerElement: ElementRef;

    constructor(
            private loader: DynamicComponentLoader,
            private injector: Injector,
            private element: ElementRef,
            @ViewQuery('realContainer') private containers: QueryList<ElementRef>
        ) {
    }
    loadComponent() {
        this.realContainerElement = this.containers.first;

        console.log('Loading...');
        //const loaded = this.loader.loadAsRoot(ChildComponent, '#container', this.injector);
        const loaded = this.loader.loadIntoLocation(ChildComponent, this.element, 'container');
        loaded.then((componentRef) => {
            //console.dir(componentRef);
            //console.dir(componentRef.location.nativeElement);
            //console.log(componentRef.instance);
            this.realContainerElement.nativeElement.appendChild(componentRef.location.nativeElement);
        });
    }
}