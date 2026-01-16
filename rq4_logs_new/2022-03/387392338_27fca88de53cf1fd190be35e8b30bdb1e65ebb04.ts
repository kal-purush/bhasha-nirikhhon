import { Location } from "@angular/common";
import { Component } from "@angular/core";
import { FormControl, Validators } from "@angular/forms";
import { MatDialog } from "@angular/material/dialog";
import { MatTableDataSource } from "@angular/material/table";
import { Router } from "@angular/router";
import { DialogConfigService } from "../dialogs/dialog-config.service";
import { DisagreeingAnnotationsDialogComponent } from "../dialogs/disagreeing-annotations-dialog/disagreeing-annotations-dialog.component";
import { Annotation } from "../model/annotation/annotation.model";
import { DataSetProject } from "../model/data-set-project/data-set-project.model";
import { DataSet } from "../model/data-set/data-set.model";
import { AnnotationStatus } from "../model/enums/enums.model";
import { Instance } from "../model/instance/instance.model";
import { AnnotationService } from "../services/annotation.service";
import { DataSetProjectService } from "../services/data-set-project.service";
import { NotificationService } from "../services/shared/notification.service";
import { LocalStorageService } from "../services/shared/local-storage.service";

@Component({
    selector: 'de-instances',
    templateUrl: './instances.component.html',
    styleUrls: ['./instances.component.css']
})
  
export class InstancesComponent {

    public chosenProject: DataSetProject = new DataSetProject();
    public chosenDataset: DataSet = new DataSet();
    public chosenInstance: Instance = new Instance(this.storageService);

    public dataSource: MatTableDataSource<Instance> = new MatTableDataSource<Instance>();
    private initColumns: string[] = ['codeSnippetId', 'annotated', 'severity'];
    public displayedColumns: string[] = this.initColumns;
    public searchInput: string = '';
    public selectedAnnotationStatus: AnnotationStatus = AnnotationStatus.All;
    public annotationStatuses: string[] = Object.keys(AnnotationStatus);
    public selectedSeverity: number | null = null;
    public severityValues: Set<number|null> = new Set();
    public codeSmells: string[] = [];
    public selectedSmellFormControl = new FormControl('', Validators.required);
    public filter: string = 'All instances';
    
    constructor(private dialog: MatDialog, private router: Router,
      public storageService: LocalStorageService, private annotationService: AnnotationService,
      private annotationNotificationService: NotificationService,
      private projectService: DataSetProjectService, private location: Location) {
        this.annotationNotificationService.datasetChosen.subscribe((dataset: DataSet) => {
          this.chosenDataset = dataset;
        });
        this.annotationNotificationService.projectChosen.subscribe((res: any) => {
          this.chosenProject = res['project'];
          this.filter = res['filter'];
          this.filterInstances(this.chosenProject.id).then(() => {
            this.chooseDefaultInstance();
          });
        });
        this.annotationNotificationService.instanceChosen.subscribe(instance => {
          this.chosenInstance = instance;
          this.updateCandidates();
        });
        this.annotationNotificationService.nextInstance.subscribe(() => {
          this.loadNextInstance();
        });
        this.annotationNotificationService.previousInstance.subscribe(() => {
          this.loadPreviousInstance();
        });
        this.annotationNotificationService.newAnnotation.subscribe(annotation => {
          this.annotationSubmitted(annotation);
        });
        this.annotationNotificationService.changedAnnotation.subscribe(annotation => {
          this.annotationSubmitted(annotation);
        });
    }

    private async updateCandidates() {
      this.chosenProject = new DataSetProject(await this.projectService.getProject(this.chosenInstance.projectId));
      this.codeSmells = [];
      this.chosenProject.candidateInstances.forEach(candidate => this.codeSmells.push(candidate.codeSmell?.name!));
      this.chosenProject.candidateInstances.forEach(candidate => {
        candidate.instances.forEach((instance, index) => {
          candidate.instances[index] = new Instance(this.storageService, instance);
          if (instance.id == this.chosenInstance.id) {
            this.storageService.setSmellFilter(candidate.codeSmell?.name!);
            this.selectedSmellFormControl.setValue(candidate.codeSmell?.name);
          }
        });
      });
      this.dataSource.data = this.filterInstancesBySmell();
      this.initSeverities();
    }

    private annotationSubmitted(annotation: Annotation) {
      this.updateInstancesTable(annotation);
      this.initSeverities();
      if (this.storageService.getAutoAnnotationMode() == 'true') this.loadNextInstance();
    }

    private updateInstancesTable(annotation: Annotation) {
      this.storageService.setSmellFilter(annotation.instanceSmell.name);
      var annotatedInstance = this.filterInstancesBySmell().find(i => i.id == this.chosenInstance.id);
      if (!annotatedInstance) return;
      annotatedInstance.annotationFromLoggedUser = annotation;
      annotatedInstance.hasAnnotationFromLoggedUser = true;
    }

    public chooseInstance(id: number): void {
      this.chosenInstance.id = id;
      this.router.navigate(['datasets/' + this.chosenDataset.id + '/instances', id]);
    }

    public searchInstances(): void {
      var instances = this.filterInstancesBySmell();
      if (this.selectedSeverity != null) {
        var instancesWithSelectedSeverity = instances.filter(i => i.annotationFromLoggedUser?.severity == this.selectedSeverity)!;
        this.dataSource.data = this.filterInstancesByName(instancesWithSelectedSeverity);
      } else {
        this.dataSource.data = this.filterInstancesByName(instances);
      }
    }

    private filterInstancesByName(instances: Instance[]) {
      return instances.filter(i => i.codeSnippetId.toLowerCase().includes(this.searchInput.toLowerCase()))!;
    }

    public filterByAnnotationStatus() {
      var instances = this.filterInstancesBySmell();
      switch (this.selectedAnnotationStatus) {
        case AnnotationStatus.Annotated: {
          this.dataSource.data = instances.filter(i => i.hasAnnotationFromLoggedUser)!;
          return;
        }
        case AnnotationStatus.Not_Annotated: { 
          this.dataSource.data = instances.filter(i => !i.hasAnnotationFromLoggedUser)!;
          return;
        } 
        case AnnotationStatus.All: { 
          this.dataSource.data = instances;
        } 
      }
    }

    public filterBySeverity() {
      this.selectedAnnotationStatus = AnnotationStatus.All;
      var instances = this.filterInstancesBySmell();
      if (this.selectedSeverity == null) this.dataSource.data = instances;
      else this.dataSource.data = instances.filter(i => i.annotationFromLoggedUser?.severity == this.selectedSeverity)!;
    }

    public showAllAnnotations(annotations: Annotation[], instanceId: number): void {
      let dialogConfig = DialogConfigService.setDialogConfig('auto', 'auto', {annotations: annotations, instanceId: instanceId});
      this.dialog.open(DisagreeingAnnotationsDialogComponent, dialogConfig);
    }

    public smellSelectionChanged() {
      this.searchInput = '';
      this.storageService.setSmellFilter(this.selectedSmellFormControl.value);
      this.dataSource.data = this.filterInstancesBySmell();
      this.initSeverities();
      this.selectedAnnotationStatus = AnnotationStatus.All;
      this.chooseDefaultInstance();
    }

    private filterInstancesBySmell(): Instance[] {
      return this.chosenProject.candidateInstances.find(c => c.codeSmell?.name == this.storageService.getSmellFilter())?.instances!;
    }

    private initSeverities() {
      this.severityValues.clear();
      this.severityValues.add(null);
      this.dataSource.data.forEach((i:any) => {
        if (i.annotationFromLoggedUser?.severity != null) this.severityValues.add(i.annotationFromLoggedUser?.severity);
      });
    }

    private chooseDefaultInstance() {
      if (this.chosenProject.hasInstances()) this.chooseInstance(this.dataSource.data[0].id);
    }

    public async filterInstances(id: number) {
      if (this.filter == 'All instances') {
        this.chosenProject = new DataSetProject(await this.projectService.getProject(id));
      } else if (this.filter == 'Need additional annotations') {
        this.chosenProject.candidateInstances = await this.annotationService.requiringAdditionalAnnotation(id);
      } else {
        this.chosenProject.candidateInstances = await this.annotationService.disagreeingAnnotations(id);
      }
      this.initSmellSelection();
      this.initInstances();
      this.initSeverities();
    }

    private initSmellSelection() {
      this.codeSmells = [];
      this.chosenProject.candidateInstances.forEach(candidate => this.codeSmells.push(candidate.codeSmell?.name!));
      var currentSmellFilter = this.storageService.getSmellFilter();
      if (!currentSmellFilter) {
        this.storageService.setSmellFilter(this.codeSmells[0]);
        this.selectedSmellFormControl.setValue(this.codeSmells[0]);
      } else  {
        this.selectedSmellFormControl.setValue(currentSmellFilter);
      }
    }

    private initInstances() {
      this.searchInput = '';
      this.chosenProject.candidateInstances.forEach(candidate => {
        candidate.instances.forEach((instance, index) => candidate.instances[index] = new Instance(this.storageService, instance));
      });
      this.dataSource.data = this.filterInstancesBySmell();
    }

    public loadPreviousInstance() {
      var projectInstances = this.getProjectInstances(this.chosenProject);
      var currentInstanceIndex = projectInstances.findIndex(i => i.id == this.chosenInstance.id);
      if (currentInstanceIndex == 0) this.loadPreviousProjectInstance();
      else this.chooseInstance(projectInstances[currentInstanceIndex-1].id);
    }

    private getProjectInstances(project: DataSetProject): Instance[] {
      var allInstances: Instance[] = [];
      project.candidateInstances.forEach(candidate => {
        if (candidate.codeSmell?.name == this.selectedSmellFormControl.value) {
          candidate.instances.forEach(instance => {
            allInstances.push(instance);
          });
        }
      });
      return allInstances;
    }

    private async loadPreviousProjectInstance() {
      var previousProject = this.getPreviousProject();
      if (!previousProject) return;
      else this.chosenProject = previousProject;

      this.chosenProject.candidateInstances = new DataSetProject(await this.projectService.getProject(previousProject.id)).candidateInstances;
      this.initSmellSelection();
      this.initInstances();
      this.initSeverities();
      this.getLastInstance();
    }

    private getPreviousProject() {
      var currentProjectId = this.chosenDataset.projects.findIndex(p => p.id == this.chosenProject.id);
      if (currentProjectId == 0) return null;
      else return this.chosenDataset.projects[currentProjectId-1];
    }

    private getLastInstance() {
      var projectInstances = this.getProjectInstances(this.chosenProject);
      if (projectInstances.length > 0) {
        this.chosenInstance = projectInstances[projectInstances.length-1];
        this.router.navigate(['datasets/' + this.chosenDataset.id + '/instances', this.chosenInstance.id]);
      }
    }

    public loadNextInstance() {
      var projectInstances = this.getProjectInstances(this.chosenProject);
      var currentInstanceIndex = projectInstances.findIndex(i => i.id == this.chosenInstance.id);
      if (currentInstanceIndex == projectInstances.length-1) this.loadNextProjectInstance();
      else this.chooseInstance(projectInstances[currentInstanceIndex+1].id);
    }

    private loadNextProjectInstance() {
      var nextProject = this.getNextProject();
      if (nextProject) { 
        this.filterInstances(nextProject.id).then(() => {
            this.chooseDefaultInstance();
        }).then(() => {
            var projectInstances = this.getProjectInstances(this.chosenProject);
            if (projectInstances.length > 0) {
            this.chosenInstance = projectInstances[0];
            this.annotationNotificationService.instanceChosen.emit(this.chosenInstance);
            this.location.replaceState('/datasets/'+this.chosenDataset.id+'/instances/'+this.chosenInstance.id);
            }
        });
      }
    }

    private getNextProject() {
      var currentProjectId = this.chosenDataset.projects.findIndex(p => p.id == this.chosenProject.id);
      if (currentProjectId == this.chosenDataset.projects.length-1) return null;
      else return this.chosenDataset.projects[currentProjectId+1];
    }
}