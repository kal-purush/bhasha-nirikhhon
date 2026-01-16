import { ComponentRef, ComponentResolver, OnDestroy } from "@angular/core";
import { ModalService } from './modal.service';
import { Subject } from "rxjs/Rx";
export declare class SkyModal implements OnDestroy {
    private resolver;
    private modalService;
    target: any;
    subject: Subject<any>;
    cmpRef: ComponentRef<any>;
    visible: boolean;
    escKey(): void;
    constructor(resolver: ComponentResolver, modalService: ModalService);
    resolveComponent(component: any, request: any): void;
    ngOnDestroy(): void;
    show(component: any, request: any, config: any): Subject<any>;
    hide(): void;
}