import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { IonicModule } from '@ionic/angular';

import { LichsuPage } from './lichsu.page';

describe('LichsuPage', () => {
  let component: LichsuPage;
  let fixture: ComponentFixture<LichsuPage>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LichsuPage ],
      imports: [IonicModule.forRoot()]
    }).compileComponents();

    fixture = TestBed.createComponent(LichsuPage);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }));

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});