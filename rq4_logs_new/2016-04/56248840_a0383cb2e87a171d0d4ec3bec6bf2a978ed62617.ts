import {Component, OnInit, Input} from 'angular2/core';
import {PersonService} from './person.service';
import {Configuration} from './configuration'
import {CORE_DIRECTIVES} from 'angular2/common';
import {Person} from './person.interface';

@Component({
  selector: 'children',
  templateUrl: 'app/children.component.html',
  // inputs: ['url'],
  directives: [CORE_DIRECTIVES]
})

export class ChildrenComponent implements OnInit {
  public children: Person[];
  public errorMessage: string;
  @Input() myurl: string;

  constructor(private _http: PersonService) { }
  
  ngOnInit() {
        this.getChildren(this.myurl);
    };

  private getChildren(url) {
        this._http.getChildren(url)
            .subscribe(
            data => this.children = data,
            error => this.errorMessage = <any>error,
            () => console.log("Finished retrieving children.")
            );
    }
}