import {Component, View} from 'angular2/core';

@Component({
    selector: 'ng2-table'
})
@View({
    templateUrl: 'app/table.html'
})

export class AppComponent {
    public title = 'Dashboard';
    public Resource = Resource;
    public keys = keys;
}

const Resource = [
    {id: 1, name: "John Travolta1", age: 12, created: "1987-09-08"},
    {id: 2, name: "John Travolta2", age: 32, created: "1967-09-08"},
    {id: 3, name: "John Travolta3", age: 42, created: "1957-09-08"},
    {id: 4, name: "John Travolta4", age: 52, created: "1927-09-08"},
    {id: 5, name: "John Travolta5", age: 62, created: "1937-09-08"},
];

const keys = Object.keys(Resource[0]);