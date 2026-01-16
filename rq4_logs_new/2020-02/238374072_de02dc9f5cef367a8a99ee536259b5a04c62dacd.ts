import {Component, Inject, OnInit, ViewEncapsulation} from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from "@angular/material/dialog";
import {GameSession} from "./gamesession";
import {FormBuilder, Validators, FormGroup} from "@angular/forms";

@Component({
    selector: 'player-dialog',
    templateUrl: './player-dialog.component.html',
    styleUrls: ['./player-dialog.component.css']
})
export class PlayerDialogComponent implements OnInit {

    form: FormGroup;
    nrplayers:string;

    constructor(
        private fb: FormBuilder,
        private dialogRef: MatDialogRef<PlayerDialogComponent>,
        @Inject(MAT_DIALOG_DATA) {nrplayers}:GameSession ) {

        this.nrplayers = nrplayers;


        this.form = fb.group({
            nrplayers: [nrplayers, Validators.required]
        });

    }

    ngOnInit() {

    }


    save() {
        this.dialogRef.close(this.form.value);
    }

    close() {
        this.dialogRef.close();
    }

}