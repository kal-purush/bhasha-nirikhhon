import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { IonicModule } from '@ionic/angular';

import { EModalPage } from './e-modal.page';

describe('EModalPage', () => {
  let component: EModalPage;
  let fixture: ComponentFixture<EModalPage>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EModalPage ],
      imports: [IonicModule.forRoot()]
    }).compileComponents();

    fixture = TestBed.createComponent(EModalPage);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }));

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});