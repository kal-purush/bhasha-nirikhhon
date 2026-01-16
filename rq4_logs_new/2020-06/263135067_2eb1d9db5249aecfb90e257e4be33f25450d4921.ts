import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { IonicModule } from '@ionic/angular';

import { InvitadoPage } from './invitado.page';

describe('InvitadoPage', () => {
  let component: InvitadoPage;
  let fixture: ComponentFixture<InvitadoPage>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ InvitadoPage ],
      imports: [IonicModule.forRoot()]
    }).compileComponents();

    fixture = TestBed.createComponent(InvitadoPage);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }));

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});