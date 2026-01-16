import { Message } from 'primeng/api';
import { Location } from "@angular/common";
import { ActivatedRoute, Params } from "@angular/router";

export class BaseComponent {
    constructor(
        private baseActivatedRoute: ActivatedRoute,
        private baseLocation: Location
    ) { }

    messages: Message[];
    isInEditMode: boolean = true;

    showMessage(isEditable: boolean, severity: string, summary: string, shouldGoBack: boolean, msg: string): void {
        this.isInEditMode = isEditable;
        this.messages = [];
        this.messages.push({
            severity: severity, summary: summary, detail: msg
        });
        if (shouldGoBack) {
            setTimeout(() => {
                this.goBack();
            }, 3000 )
        }
    }
    goBack(): void {
        this.baseLocation.back();
    }
}