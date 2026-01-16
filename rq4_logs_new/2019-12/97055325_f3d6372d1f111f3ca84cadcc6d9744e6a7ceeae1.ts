import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OldVeloGridComponent } from './old-velo-grid.component';

describe('VeloGridComponent', () => {
  let component: OldVeloGridComponent;
  let fixture: ComponentFixture<OldVeloGridComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ OldVeloGridComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OldVeloGridComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  // RESTORE
  // it('should be created', () => {
  //   expect(component).toBeTruthy();
  // });
});