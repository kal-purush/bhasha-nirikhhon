import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { IonicModule } from '@ionic/angular';

import { EditTeamPage } from './edit-team.page';

describe('EditTeamPage', () => {
  let component: EditTeamPage;
  let fixture: ComponentFixture<EditTeamPage>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EditTeamPage ],
      imports: [IonicModule.forRoot()]
    }).compileComponents();

    fixture = TestBed.createComponent(EditTeamPage);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }));

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});