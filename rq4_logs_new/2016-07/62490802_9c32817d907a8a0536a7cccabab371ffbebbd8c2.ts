import {autoinject}      from 'aurelia-framework';
import {Router}          from 'aurelia-router';
import {withModal}       from '../shared/decorators';
import {ProjectFinder}   from '../shared/project-finder';
import {ScaffoldProject} from '../scaffolding/scaffold-project';

@autoinject()
export class Landing {

  constructor(private projectFinder: ProjectFinder,
              private router: Router) {
  }

  async open() {
    if (await this.projectFinder.openDialog()) {
      this.router.navigateToRoute('main');
    }
  }

  @withModal(ScaffoldProject)
  create(projectPath) {
    this.router.navigateToRoute('main');
  }
}