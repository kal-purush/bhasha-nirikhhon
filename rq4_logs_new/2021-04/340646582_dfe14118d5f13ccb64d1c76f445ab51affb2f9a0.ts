import {Component, Inject, OnInit} from '@angular/core';
import {ActivatedRoute} from "@angular/router";
import {TrailerService} from "../services/trailer.service";
import {NotificationService} from "../services/notification.service";
import {MAT_DIALOG_DATA, MatDialogRef} from "@angular/material/dialog";
import {DialogData} from "../profile/profile.component";
import {Box} from "../model/box.model";
import {AuthService} from "../services/auth.service";
import {TypeCargo} from "../model/typeCargo.model";
import {TypeCargoService} from "../services/typeCargo.service";
import {BoxService} from "../services/box.service";

@Component({
  selector: 'app-create-box',
  templateUrl: './create-box.component.html',
  styleUrls: ['./create-box.component.css']
})
export class CreateBoxComponent implements OnInit {

  constructor(private route: ActivatedRoute,
              private authService: AuthService,
              public typeService: TypeCargoService,
              private boxService: BoxService,
              public notificationService: NotificationService,
              public dialogRef: MatDialogRef<CreateBoxComponent>,
              @Inject(MAT_DIALOG_DATA) public data: DialogData) {
  }
  public typeCargoList: TypeCargo[] = [];
  newBox: Box;

  tId = null;
  name = null;
  height = null;
  width = null;
  weight = null;
  searchBoxByClientEmail: string = this.authService.getAuthEmail();

  ngOnInit(): void {
    this.typeService.getType().subscribe((data:TypeCargo[])=>{this.typeCargoList = data});
    this.newBox.client.userId = parseInt(this.authService.getClientId());
  }
  onSubmit() {
    this.boxService.create(this.newBox)
      .subscribe((response) => {
        if (response.status === 200) {
          this.dialogRef.close("Yes");
          this.notificationService.add('successfulUpdate');
          setTimeout(()=>{this.notificationService.remove('successfulUpdate')}, 2000);
        }
      },  error => {
        this.dialogRef.close("error");
      });
  }

  onNoClick(): void {
    this.dialogRef.close("cancel");
  }
}