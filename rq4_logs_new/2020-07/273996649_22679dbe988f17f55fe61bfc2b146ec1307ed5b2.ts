import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { IonicModule } from '@ionic/angular';

import { PanelPage } from './panel.page';

describe('PanelPage', () => {
  let component: PanelPage;
  let fixture: ComponentFixture<PanelPage>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PanelPage ],
      imports: [IonicModule.forRoot()]
    }).compileComponents();

    fixture = TestBed.createComponent(PanelPage);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }));

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});