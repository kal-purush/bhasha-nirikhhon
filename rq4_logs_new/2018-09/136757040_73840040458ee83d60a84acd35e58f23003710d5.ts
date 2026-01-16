import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OverlayLayoutComponent } from './overlay-layout.component';
import { SpinnerComponent } from '../spinner';
import { ServicesModule } from '../../services';

describe('OverlayLayoutComponent', () => {
  let component: OverlayLayoutComponent;
  let fixture: ComponentFixture<OverlayLayoutComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        ServicesModule
      ],
      declarations: [
        SpinnerComponent,
        OverlayLayoutComponent
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OverlayLayoutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});