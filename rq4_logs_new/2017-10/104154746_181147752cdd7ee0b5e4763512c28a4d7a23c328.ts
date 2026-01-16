import { Component, OnInit } from '@angular/core';

import { Project } from '../../_model/project.model';
import { ProjectService } from '../../projects/_service/project.service';

@Component({
  selector: 'nr-admin-projects',
  templateUrl: './admin-projects.component.html'
})
export class AdminProjectsComponent implements OnInit {

  inputFile = '';

  projects: Project[] = [];

  isLoading$ = false;

  project_aux: Project = { name: '', description: '' };

  base64textString = '';

  evt1 = null;

  constructor(private _projectService: ProjectService) { }

  ngOnInit() {
  }

  // ----- ChooseFile and convert to base64 ------
  handleFileSelect(evt) {

    console.log();

    const files = evt.target.files;
    const file = files[0];

    if (files && file) {
      const reader = new FileReader();

      reader.onload = this._handleReaderLoaded.bind(this);

      reader.readAsBinaryString(file);
    }

    this.evt1 = evt;
  }
  _handleReaderLoaded(readerEvt) {
    const binaryString = readerEvt.target.result;
    this.base64textString = btoa(binaryString);
    console.log(btoa(binaryString));
  }
  // -----------------PROJECTS--------------------------------- !
  // Estos metodos los podriamos tener en un servicio aparte.


  uploadProject(): void {

    this.project_aux.imageEncoded = this.base64textString;
    // Subir imagen
    // this._projectService.

    if (this.base64textString !== '' && this.project_aux.name !== '') {
      this._projectService.insert(this.project_aux).subscribe(data => {

        if (data.status === 201) {
          alert('Proyecto cargado correctamente');
          console.log('project_aux');
          console.log(this.project_aux);

          // clean
          this.project_aux = { name: '', description: '', imageEncoded: null, imageDecoded: null };
          this.base64textString = '';

          this.evt1.srcElement.value = '';


        } else {
          alert('El proyecto no pudo ser cargado -- http code ${data.status}');
        }

      });



    } else {
      alert('No se ingresó nombre o imagen del proyecto, revise sus datos.');
    }

  }

  listProjects(): void {
    this.isLoading$ = true;
    this._projectService.list().subscribe(
      data => {
        this.projects = data;
        this.isLoading$ = false;
      }
    );

  }

  deleteProject(index: number): void {
    this._projectService.delete(this.projects[index]).subscribe(data => {
      if (data.status === 200) {
        alert('Proyecto eliminado correctamente');
        this.projects.splice(index, 1);
      } else {
        alert('El proyecto no pudo ser eliminado');
      }


    });
  }


  updateDescription(description: string): void {
    if (description.length > 254) {
      this.project_aux.description = description.substring(0, 251) + '...';
    } else {
      this.project_aux.description = description;
    }

  }

}
