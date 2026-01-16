import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { IonicModule } from '@ionic/angular';

import { KetquaPage } from './ketqua.page';

describe('KetquaPage', () => {
  let component: KetquaPage;
  let fixture: ComponentFixture<KetquaPage>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ KetquaPage ],
      imports: [IonicModule.forRoot()]
    }).compileComponents();

    fixture = TestBed.createComponent(KetquaPage);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }));

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});