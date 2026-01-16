import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ClusterItemComponent } from './cluster-item.component';

describe('ClusterItemComponent', () => {
  let component: ClusterItemComponent;
  let fixture: ComponentFixture<ClusterItemComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ClusterItemComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ClusterItemComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});