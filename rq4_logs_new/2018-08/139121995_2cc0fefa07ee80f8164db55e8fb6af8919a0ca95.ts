import { Component, OnInit, Output, EventEmitter, Input } from '@angular/core';
import { FormGroup, FormBuilder, Validators } from '@angular/forms';

@Component({
    selector: 'app-dashboard-tab',
    templateUrl: './dashboard-tab.component.html',
    styleUrls: ['./dashboard-tab.component.css']
})
export class DashboardTabComponent implements OnInit {
    @Input() public stepLabel: string;
    @Input() public inputPlaceholder: string;
    @Input() public buttonLabel: string;
    @Output() private actionRequest: EventEmitter<string> = new EventEmitter<string>();
    public formGroup: FormGroup;

    constructor (private formBuilder: FormBuilder) { }

    ngOnInit () {
        this.formGroup = this.formBuilder.group({
            formControl: ['', Validators.required]
        });
    }

    public buttonClicked () {
        if (!this.formGroup.invalid) {
            const roomName = this.formGroup.value.formControl as string;
            this.actionRequest.emit(roomName);
        }
    }

}