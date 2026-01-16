import {Component, ElementRef, bootstrap, OnDestroy, OnInit, FORM_DIRECTIVES, CORE_DIRECTIVES} from 'angular2/angular2';
import {Http, Response, HTTP_PROVIDERS} from 'angular2/http';
import {Alert} from 'deps/ng2-bootstrap/ng2-bootstrap.ts'

class Project {
    public id;
    public name;
}

@Component({
    selector: '[shelf-app]',
    directives: [Alert, FORM_DIRECTIVES, CORE_DIRECTIVES],
    template: `
    <h1>Welcome to Shelf</h1>

    <alert [type]="'warning'" dismissible="true">This tool is under development.</alert>

    <ul>
        <li *ng-for="#project of projects" >
            <span class="project-title">{{project.name}}</span> <button class="btn btn-danger" (click)="deleteProject(project)">Delete</button>
        </li>
    </ul>
    <button class="btn btn-primary">New Project</button>
    `,
    styles: [`
    h1 {font-color: #aaa;}
    ul {padding: 0;}
    ul li { list-style: none; margin: 5px 0;}
    .project-title { font-size: 2.4em; }
    `],
})
class ShelfApp implements OnInit, OnDestroy{
    private projects: Project[];

    constructor(private http: Http) {
        this.refreshProjectList();

    }

    refreshProjectList() {
        this.http.get('/api/projects/').subscribe(res => this.projects = res.json());
    }

    deleteProject(p: Project) {
        this.http.delete('/api/projects/' + p.id)
            .subscribe(response => this.projectDeleted(response));
    }

    projectDeleted(resp) {
        console.log(resp);
        this.refreshProjectList();
    }
}

bootstrap(ShelfApp, HTTP_PROVIDERS);