import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { IonicModule } from '@ionic/angular';

import { ModalTPage } from './modal-t.page';

describe('ModalTPage', () => {
  let component: ModalTPage;
  let fixture: ComponentFixture<ModalTPage>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ModalTPage ],
      imports: [IonicModule.forRoot()]
    }).compileComponents();

    fixture = TestBed.createComponent(ModalTPage);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }));

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});