import { AppModule } from './../app.module';
import { LoginComponent } from './../login/login.component';
import { TurmaService } from './turma.service';
import { async, ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';

import { TurmasComponent, CriarTurmasDialogo } from './turmas.component';
import { DebugElement } from '@angular/core';
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { RouterTestingModule } from '@angular/router/testing';
import { environment } from 'src/environments/environment';
import {MatDialogRef, MatDialogModule, MAT_DIALOG_DATA} from '@angular/material/dialog';



describe('TurmasComponent', () => {
  let component: TurmasComponent;
  let fixture: ComponentFixture<TurmasComponent>;
  let dialogo: CriarTurmasDialogo;
  let fixture_dialogo: ComponentFixture<CriarTurmasDialogo>;
  let httpMock: HttpTestingController;
  let service: TurmaService;
  let injector: TestBed;

  let dialogMock = {
    close: () => { }
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        AppModule,
        HttpClientTestingModule,
        RouterTestingModule,
        MatDialogModule,
      ],
      providers: [
        {provide: MatDialogRef, useValue: dialogMock},
        {provide: MAT_DIALOG_DATA, useValue: {}},
      ]
    })
    .compileComponents().then(() => {
      fixture = TestBed.createComponent(TurmasComponent);  
      component = fixture.componentInstance;
      fixture_dialogo = TestBed.createComponent(CriarTurmasDialogo); 
      dialogo = fixture_dialogo.componentInstance;
    });
    injector = getTestBed();
    service = injector.get(TurmaService);
    httpMock = TestBed.get(HttpTestingController);
  }));

  afterEach(() => {
    httpMock.verify();
  });

  const listaTurmasTeste = {
    data: [
      { id: 1, descricao: 'A' },
      { id: 2, descricao: 'B' },
      { id: 3, descricao: 'C' },
      { id: 4, descricao: 'G' },
    ]
  };

  const turmaE = {
    data:
    { descricao: 'E' }
  };

  const turmaDetails = {
    data: [
      { id: 1, descricao: 'A' },
    ]
  };

  const turma = { id: 1, descricao: 'A' };

  it('get_turmas() should return data', () => {
    service.get_turmas().subscribe((res:any) => {
      expect(res).toEqual(listaTurmasTeste);
    });
    const req = httpMock.match(environment.urlApi + 'turmas/');
    expect(req[0].request.method).toBe('GET');
    req[0].flush(listaTurmasTeste);
  });


  it('getById() should return turma detail', () => {
    // const req = httpMock.match({method: 'GET', url: environment.urlApi + 'turma/1'});
    service.getById('1').subscribe((data: any) => {
      console.log(data);
      expect(data[0].descricao).toEqual('A');
    });
    const req = httpMock.match(environment.urlApi + 'turma/1');
    console.log(req[0].request.method);
    expect(req[0].request.method).toBe('GET');
    // req[0].flush(turmaDetails);
  });

  it('create_turmas() should POST and return data', () => {
    service.create_turmas(turmaE).subscribe((res) => {
      expect(res).toEqual({ msg: 'success' });
    });
    const req = httpMock.expectOne(environment.urlApi + 'turma/');
    expect(req.request.method).toBe('POST');
    req.flush({ msg: 'success' });
  });


  it('should post the correct data', () => {
    service.create_turmas({ descricao: 'F' }).subscribe((data: any) => {
        expect(data.descricao).toBe('F');
      });

    const req = httpMock.expectOne(
      environment.urlApi + `turma/`,
      'post to api'
    );
    expect(req.request.method).toBe('POST');

    req.flush({
      descricao: 'F'
    });

    httpMock.verify();
  });

  it('should put the correct data', () => {
    service.edit_turmas(1, { descricao: 'G' }).subscribe((data: any) => {
        expect(data.descricao).toBe('G');
      });

    const req = httpMock.expectOne(
      environment.urlApi + `turma/1`,
      'put to api'
    );
    expect(req.request.method).toBe('PUT');

    req.flush({
      descricao: 'G'
    });

    httpMock.verify();
  });

  it('should delete the correct data', () => {
    service.delete_turmas(3).subscribe((data: any) => {
      expect(data).toBe(3);
    });

    const req = httpMock.expectOne(
      environment.urlApi + `turma/3`,
      'delete to api'
    );
    expect(req.request.method).toBe('DELETE');

    req.flush(3);

    httpMock.verify();
  });

  it('form should be invalid', async(() => {
    component.formularioTurma.controls['id'].setValue('');
    component.formularioTurma.controls['descricao'].setValue('');
    expect(component.formularioTurma.valid).toBeFalsy();
  }));

  it('form should be valid', async(() => {
    component.formularioTurma.controls['id'].setValue('4');
    component.formularioTurma.controls['descricao'].setValue('D');
    expect(component.formularioTurma.valid).toBeTruthy();
  }));
});