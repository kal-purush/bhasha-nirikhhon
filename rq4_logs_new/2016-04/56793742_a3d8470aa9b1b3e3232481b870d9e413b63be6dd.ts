/**
 * Created by zorodzay on 2016/04/24.
 */
import {Component} from 'angular2/core';
import {bootstrap} from 'angular2/platform/browser';

@Component({
    selector: 'report-tender-fraud-complaint',
    'template': `
    <form class="ui large form segment centered">
    <h3 class="ui header">Report suspicious tender fraud</h3>
     <div class="field">
     <label>Your Full Names</label>
     <input type="text" placeholder="Your full name" [(ngModel)]="tenderFraudComplaint.complaintName" required> 
</div>
<div class="field">
<label>Your Email Address</label>
<input type="email" placeholder="email@domain.com" [(ngModel)]="tenderFraudComplaint.complaintEmail">
</div>
<div class="field">
<label>Your Phone Number</label>
<input type="tel" placeholder="phone number" [(ngModel)]="tenderFraudComplaint.complaintPhone">  
</div>
<div class="field">
<label>Your Id Number</label>
<input type="text" placeholder="Your Id Or Passport Number" [(ngModel)]="tenderFraudComplaint.complaintId">
</div>
<div class="field">
<label>Priority</label>
<select [(ngModel)]="tenderFraudComplaint.priority">
<option *ngFor="#priority of priorities" [value]="priority">{{priority}}</option>
</select>
</div>
<div class="field">
<label>Offending Company</label>
<input  type="text" [(ngModel)]="tenderFraudComplaint.offendingCompany"required>
</div>
<div class="field">
<label>Details Of The Complaint</label>
<textarea [(ngModel)]="tenderFraudComplaint.complaint"></textarea>
</div>
<div> 
<button class="ui button secondary right" type="reset">Reset</button>
<button class="ui button primary right" type="button" (click)="saveFraudComplaint()">Save Complaint</button>
</div>
</form>
`
})
class ReportTenderFraudComplaintComponent {
    tenderFraudComplaint:any = {};
    priorities:String[] = ["High", "Medium", "Low"];

    saveFraudComplaint():void {
        console.log("Saving complaint", JSON.stringify(this.tenderFraudComplaint));
    }
}

bootstrap(ReportTenderFraudComplaintComponent, []);