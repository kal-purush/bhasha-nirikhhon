import { Component, OnInit, ViewChild  } from '@angular/core';
import { CloudData, CloudOptions, TagCloudComponent } from 'angular-tag-cloud-module';
import { ApiServiceService } from '../../services/api-service.service';

@Component({
  selector: 'app-cloudtag',
  templateUrl: './cloudtag.component.html',
  styleUrls: ['./cloudtag.component.css']
})



export class CloudtagComponent {

@ViewChild(TagCloudComponent) tagCloudComponent: TagCloudComponent;

options: CloudOptions = {
  width : 0.8,
  height : 400,
  overflow: false,
  zoomOnHover: {
    scale: 1.3,
    transitionTime: 1.2
  }
};

elpais: CloudData[];
elmundo: CloudData[];
abc: CloudData[];
diarioes: CloudData[];

  constructor(private apiService: ApiServiceService) {
    this.apiService.getCloudTagss().subscribe(
      resp => {
        resp = resp.json()['datos'];
        this.elpais = resp[0]['elpais'];
        this.diarioes = resp[1]['diarioes'];
        this.elmundo = resp[2]['elmundo'];
        this.abc = resp[3]['abc'];
      },
      error => {
        console.log(error);
      },
    );
  }


  log(eventType: string, e?: CloudData) {
    console.log(eventType, e);
  }

  reDraw() {
    this.tagCloudComponent.reDraw();
  }
}