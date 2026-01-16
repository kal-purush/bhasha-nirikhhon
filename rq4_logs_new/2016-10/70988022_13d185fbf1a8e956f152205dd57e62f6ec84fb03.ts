import { Component, EventEmitter, Output } from '@angular/core';
import { CharityService } from '../../services/charity.service';

@Component({
  selector: 'charity-form',
  templateUrl: './charity-form.component.html',
  styleUrls: ['./charity-form.component.css']
})

export class CharityFormComponent {
  name:string = null;
  register_id:number = null;
  description:string = null;
  
  @Output() updateCharities = new EventEmitter();
  
  constructor(private charityService:CharityService) {
  }
  
  onSubmit():void {
    let data = JSON.stringify({
      name: this.name,
      register_id: this.register_id,
      description: this.description
    });
    
    this.charityService.create(data).then(
      () => this.updateCharities.emit()
    );
  }
}