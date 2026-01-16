import { Injectable } from '@angular/core';
import { AngularFireObject } from '@angular/fire/database';
import { AngularFirestore, AngularFirestoreCollection } from '@angular/fire/firestore';
import { Project } from 'modules/project/models/project'
@Injectable({
  providedIn: 'root'
})
export class ProjectService {

  projectsRef: AngularFirestoreCollection<any>|any ;
  projectRef: AngularFireObject<any> | undefined;

  dbPath = '/project';

  constructor(private db: AngularFirestore) {
    this.projectsRef = db.collection(this.dbPath);
   }

  getAll(): AngularFirestoreCollection<Project> {
    return <any>this.projectsRef;
  }

  AddProject(project: Project) {
    return this.projectsRef?.add({...project});
  }


  UpdateProject(project: Project) {
    this.projectsRef?.update({
      title:project.title,
      stdate: project.stdate,
      eddate:project.eddate,
      department: project.department,
      personnel:project.personnel,
      ceranko: project.rank,
      management:project.management,
      checked: project.checked,
})}}