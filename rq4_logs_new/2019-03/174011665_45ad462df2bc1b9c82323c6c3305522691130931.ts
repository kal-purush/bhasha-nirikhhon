import { Component, OnInit, Input, Output, EventEmitter, OnChanges } from '@angular/core';


export class EditableComponent implements OnChanges {

  @Input() entity: any;
  
  @Input() set field(entityField: string){
    this.entityField = entityField;
    this.setOriginValue();
  };

  @Input() className: string;



  @Input() style : any;

  @Output() entityUpdated = new EventEmitter();

  public entityField: string;

  public originEntityValue: any;

  isActiveInput: boolean = false;

  constructor() { }

  ngOnChanges() {
    this.setOriginValue();
    this.isActiveInput = false;
  }

  updateEntity(){
    const entityValue =  this.entity[this.entityField];

    //check if values are different before sending to emitting an event
    if(entityValue !== this.originEntityValue){
      this.entityUpdated.emit({[this.entityField] : this.entity[this.entityField]});
      this.setOriginValue();
    }
    
    this.isActiveInput = false;
  }

  cancelUpdate(){
    this.isActiveInput = false;
    this.entity[this.entityField] = this.originEntityValue;
  }

  setOriginValue(){
    this.originEntityValue = this.entity[this.entityField];
  }

}