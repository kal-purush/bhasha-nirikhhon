import { Component, View, Input, NgClass, SimpleChange} from 'angular2/angular2';

@Component({
  selector: 'alert'
})
@View({
  templateUrl: 'src/shared/alert.html',
  directives: [NgClass]
})
export class Alert {

  @Input() message: string;
  @Input() alertType: string;

  private classMap: any;
  private iconClassMap: any;

  constructor() {


  }

  private onChanges(changes: {[propName: string]: SimpleChange}): void {
    this.classMap = {
      'alert-danger': this.alertType == "danger",
      'alert-warning': this.alertType == "warning",
      'alert-info': this.alertType == "info",
      'alert-success': this.alertType == "success"
    }
    this.iconClassMap = {
      'fa-exclamation-circle': this.alertType == "danger",
      'fa-exclamation-triangle': this.alertType == "warning",
      'fa-info-circle': this.alertType == "info",
      'fa-thumbs-up': this.alertType == "success"
    }
  }


}