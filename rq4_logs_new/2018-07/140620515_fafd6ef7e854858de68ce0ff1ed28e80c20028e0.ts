import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

@Component({
  selector: 'my-form',
  templateUrl: './my-form.component.html',
  styleUrls: ['./my-form.component.css']
})
export class MyFormComponent implements OnInit {

  fName: string;
  pwd: string;

  constructor(private _route: ActivatedRoute,
    private _router: Router) { }

  ngOnInit() {
  }

  onSubmit(): void {
    if(this.fName.toLocaleLowerCase()=='mustafa')
    this._router.navigate(['/home'])
    else{
      alert('invalid!')
    }
  }

}