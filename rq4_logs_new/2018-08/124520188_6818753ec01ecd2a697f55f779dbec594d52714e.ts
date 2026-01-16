import {Component, Input, OnDestroy, OnInit} from '@angular/core';
import {Subscription} from 'rxjs/Subscription';
import {ParamFilter} from '../../utils/paramfilter.class';

@Component({
    moduleId: module.id,
    selector: 'list-container',
    templateUrl: 'list-container.component.html',
    styleUrls: ['./list-container.component.scss']
})
export class ListContainerComponent implements OnDestroy, OnInit {

    @Input("filter") filter: ParamFilter;
    isLoadingEventSubscription: Subscription;
    public firstLoaded: boolean = false;

    ngOnInit(): void {
        this.isLoadingEventSubscription = this.filter.isLoadingEvent.subscribe(_ => {
            if (!_) {
                this.firstLoaded = true;
            }
        });
    }

    ngOnDestroy(): void {
        this.isLoadingEventSubscription.unsubscribe();
    }


}