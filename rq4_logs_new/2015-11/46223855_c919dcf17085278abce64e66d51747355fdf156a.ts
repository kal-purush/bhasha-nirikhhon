import {Component} from "angular2/angular2";
import {ProjectServices} from "../services/project_services";

@Component({
	selector: 'add-new-project',
	templateUrl: '../app/add_project/add_new_project.html'
})

export class AddNewProject {
	projects = new Array();

	constructor(private projectServices: ProjectServices) {
		projectServices.on((data: FirebaseDataSnapshot) => {
			this.projects = [];
			data.forEach((d) => {
				this.projects.push(d.val());
			});
		});
	}

	public addNewProject(event, name) {
		var project = { name: name };
		this.projectServices.push(project);
	}

}