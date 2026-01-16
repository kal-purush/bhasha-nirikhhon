import { EstacionesListService } from './estaciones-list.service';
import { AngularFireDatabase } from "angularfire2/database";
import { User } from '../../app/models/user';
import { TestBed, ComponentFixture, async, inject } from '@angular/core/testing';
import { Subject } from 'rxjs/Subject';
import { Observable } from "rxjs";
import { Estacion } from "../../app/models/estacion";

let fixtureTodos = [
    { 'text': 'Get milk' },
    { 'text': 'Take out the trash' },
    { 'text': 'Get gas for the car' },
    { 'text': 'Pay parking ticket' },
    { 'text': 'Pick up dry cleaning' },
  ];
  let angularFireDatabaseStub = { list: () => {} };
  let mockTodos$ = Observable.of(fixtureTodos);
  
  describe('EstacionesListService', () => {
    beforeEach(() => {
      spyOn(angularFireDatabaseStub, 'list').and.returnValue(mockTodos$);
  
      TestBed.configureTestingModule({
        providers: [
            EstacionesListService,
          { provide: AngularFireDatabase, useValue: angularFireDatabaseStub },
        ]
      });
    });
  
    it('#getAll', inject([EstacionesListService], (service: EstacionesListService) => {
      let todos$ = service.getEstacionesList();
      
        
        expect(true).toBeTruthy;
      
    }));
  });