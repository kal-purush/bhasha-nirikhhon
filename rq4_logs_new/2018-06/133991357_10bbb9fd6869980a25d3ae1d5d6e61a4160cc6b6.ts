import { Component, OnInit } from '@angular/core';
import { FormService } from '../form.service';

@Component({
  selector: 'admin-thing',
  styleUrls: ['./admin.component.css'],
  templateUrl: './admin.component.html'
})
export class AdminComponent implements OnInit{
  form$;
  
  constructor(private formService: FormService){
    this.form$ = this.formService.getAll();
  }

  ngOnInit(){}
}