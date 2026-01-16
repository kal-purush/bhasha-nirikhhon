import { Component, OnInit , EventEmitter, Output} from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

import { Project } from './../../../core/models/project';
import { ProjectsService } from './../../../core/services/projects.service';

@Component({
  selector: 'app-project-remove',
  templateUrl: './project-remove.component.html',
  styleUrls: ['./project-remove.component.css']
})
export class ProjectRemoveComponent implements OnInit {
    id: number;

    constructor(
        private projectsService: ProjectsService,
        private route: ActivatedRoute,
        private router: Router
    ) { }

    ngOnInit() {
        this.route.params.subscribe(params => {
            this.id = +params['id'];
        });

        this.projectsService
            .remove(this.id)
            .then(() => {
                this.router.navigate(['./projects']);
        });
    }
}