import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute, Params } from '@angular/router';

@Component({
  selector: 'app-fake',
  template: ''
})
export class FakeComponent implements OnInit {

  constructor(
    private _router: Router,
    private _route: ActivatedRoute
  ) {}

  ngOnInit() {
    let name: string;
    this._route.params.forEach((params: Params) => {
      name = params['name'];
    });

    let link: any[] = ['/game', name];
    this._router.navigate(link);
  }
}