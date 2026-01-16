import { Component, OnInit, OnDestroy } from '@angular/core';
import { AdminService } from "../../services/admin.service";
import { Subscription } from 'rxjs/Rx';
import { HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';

@Component( {
    selector: 'app-useragreement',
    templateUrl: './useragreement.component.html'
} )
export class UserAgreementComponent implements OnInit, OnDestroy {

    content: string ='';
    prevContent: string;
    mode: string = 'editor';
    options: string = '';
    userId: number;
    usrSubscription: Subscription;
    usrAgreementSubscription: Subscription;
    errorMsg: string = '';

    constructor( private adminService: AdminService, private router: Router ) { }

    ngOnInit() {
        this.getUserAgreementData();
        this.getUserData();
    }

    getUserData() {
        this.usrSubscription = this.adminService.getUser().subscribe( data => {
            this.userId = data.id;
            console.log( 'UserId: ' + this.userId );
        },
            error => {
                if ( error instanceof HttpErrorResponse ) {
                    this.errorMsg = 'SERVER ERROR: ' + error.message;
                }
                else {
                    this.errorMsg = 'Error Running Query. Please Retry';
                }
                console.log( 'ERROR IN USER AGREEMENT' );
                console.log( error );
            },
            () => {
                console.log( 'SUCCESS in USER AGREEMENT' );
            } );
    }

    getUserAgreementData() {
        this.usrAgreementSubscription = this.adminService.getUserAgreementCurrent().subscribe( data => {
            console.log( 'GOT User Agreement:' + data );
            this.content = data.text;
            this.prevContent = data.text;
        },
            error => {
                if ( error instanceof HttpErrorResponse ) {
                    this.errorMsg = 'SERVER ERROR: ' + error.message;
                }
                else {
                    this.errorMsg = 'Error Running Query. Please Retry';
                }
                console.log( 'ERROR IN USER AGREEMENT' );
                console.log( error );
            },
            () => {
                console.log( 'SUCCESS in USER AGREEMENT' );
            } );
    }

    saveAgreement() {
        if ( this.content === this.prevContent )
            this.router.navigate( ['/adminview'] );
        else {
            var data = JSON.stringify( { text: this.content } );
            this.adminService.postUserAgreement( this.userId, data ).subscribe( data => console.log( data ) );
            console.log( data );
            console.log( this.content );
            this.router.navigate( ['/adminview'] );
        }
    }

    onCancel() {
        this.router.navigate( ['/adminview'] );
    }

    ngOnDestroy(): void {
        if ( this.usrSubscription !== null ) {
            this.usrSubscription.unsubscribe();
            this.usrAgreementSubscription.unsubscribe();
        }
    }

}