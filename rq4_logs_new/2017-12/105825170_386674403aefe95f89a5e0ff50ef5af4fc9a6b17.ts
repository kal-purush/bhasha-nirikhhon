import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { IonicModule, NavController, NavParams } from 'ionic-angular';
import { By } from '@angular/platform-browser';
import { NavMock, NavParamsMock } from '../../../test-config/mocks-ionic';
import { PictureUploadPage } from './picture-upload';

describe('LandingPage', () => {
  let fixture: ComponentFixture<PictureUploadPage>;
  let component: PictureUploadPage;
  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        declarations: [PictureUploadPage],
        imports: [IonicModule.forRoot(PictureUploadPage)],
        providers: [
          { provide: NavController, useClass: NavMock },
          { provide: NavParams, useClass: NavParamsMock }
        ]
      });
      fixture = TestBed.createComponent(PictureUploadPage);
      component = fixture.componentInstance;
    })
  );

  it('should be created', () => {
    expect(component).toBeDefined();
  });
});