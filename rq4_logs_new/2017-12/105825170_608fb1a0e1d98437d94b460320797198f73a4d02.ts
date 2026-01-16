import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { IonicModule, NavController, NavParams } from 'ionic-angular';
import {
  NavMock,
  NavParamsMock,
  SearchPipeMock,
  ApiServiceProviderMock
} from '../../../test-config/mocks-ionic';
import { FaceoffPage } from './faceoff';
import { ApiServiceProvider } from '../../providers/api.service/api.service';

describe('DirectoryPage', () => {
  let fixture: ComponentFixture<FaceoffPage>;
  let component: FaceoffPage;
  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        declarations: [FaceoffPage, SearchPipeMock],
        imports: [IonicModule.forRoot(FaceoffPage)],
        providers: [
          { provide: NavController, useClass: NavMock },
          { provide: NavParams, useClass: NavParamsMock },
          { provide: ApiServiceProvider, useClass: ApiServiceProviderMock }
        ]
      });
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(FaceoffPage);
    component = fixture.componentInstance;
  });

  it('should be created', () => {
    expect(component).toBeDefined();
  });
});