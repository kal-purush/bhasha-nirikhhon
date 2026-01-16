import { Component, OnInit } from '@angular/core';
import { WebcamUtil, WebcamImage, WebcamInitError } from 'ngx-webcam';
import { Subject, Observable } from 'rxjs';

@Component({
  selector: 'app-photo',
  templateUrl: './photo.component.html',
  styleUrls: ['./photo.component.scss']
})
export class PhotoComponent implements OnInit {
  multipleWebcamsAvailable: boolean;
  showWebcam = true;
  trigger: Subject<void> = new Subject<void>();
  webcamImage: WebcamImage = null;
  errors: WebcamInitError[] = [];
  constructor() { }

  ngOnInit() {
    WebcamUtil.getAvailableVideoInputs()
    .then((mediaDevices: MediaDeviceInfo[]) => {
      this.multipleWebcamsAvailable = mediaDevices && mediaDevices.length > 1;
      this.showWebcam = true;
    })
    .catch( err => {

    });
  }
  get triggerObservable(): Observable<void> {
    return this.trigger.asObservable();
  }
  handleImage(webcamImage: WebcamImage): void {
    this.webcamImage = webcamImage;
  }
  handleInitError(error: WebcamInitError): void {
    this.errors.push(error);
  }
  triggerSnapshot(): void {
    this.trigger.next();
  }

}