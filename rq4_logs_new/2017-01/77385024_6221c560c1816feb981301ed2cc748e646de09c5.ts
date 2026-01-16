import { Component, OnInit , EventEmitter, Output} from '@angular/core';

import { Project } from './../../../core/models/project';
import { User } from './../../../core/models/user';
import { ProjectsService } from './../../../core/services/projects.service';

@Component({
  selector: 'app-project-create',
  templateUrl: './project-create.component.html',
  styleUrls: ['./project-create.component.css']
})
export class ProjectCreateComponent implements OnInit {
    model: Project;
    projects: Project[];
    title: string;
    @Output('create') emitter: EventEmitter<Project> = new EventEmitter<Project>();

    constructor(private projectsService: ProjectsService) { }

    ngOnInit() {
        this.model = new Project(0, '', '', '', '');
    }

    create() {
        const project = new Project(++this.projectsService.lastId, this.model.title, this.model.demo, this.model.github,
            this.model.image_url);

        let created = this.projectsService.add(project);
        console.log(created);
        this.emitter.emit(project);
    }
}