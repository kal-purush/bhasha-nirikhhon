import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { IonicModule } from '@ionic/angular';

import { PhantichPage } from './phantich.page';

describe('PhantichPage', () => {
  let component: PhantichPage;
  let fixture: ComponentFixture<PhantichPage>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PhantichPage ],
      imports: [IonicModule.forRoot()]
    }).compileComponents();

    fixture = TestBed.createComponent(PhantichPage);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }));

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});