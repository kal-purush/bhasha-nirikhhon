import { Component, OnInit, Input} from '@angular/core';

@Component({
  selector: 'app-join-input',
  template: `<div>
              <input class="form-control" placeholder="{{placeholder}}" type="{{type}}" name="{{name}}"/>
              <span class="star">*</span>
            </div>`,
  styleUrls: ['./join-input.component.scss']
})
export class JoinInputComponent implements OnInit {

  @Input() placeholder: string;
  @Input() type:string;
  @Input() name:string;
  
  constructor() { }

  ngOnInit(): void {
  }

}