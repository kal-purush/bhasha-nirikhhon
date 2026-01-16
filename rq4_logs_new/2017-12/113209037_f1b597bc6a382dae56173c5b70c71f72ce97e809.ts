import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SubmitArtifactComponent } from './submit-artifact.component';

describe('SubmitArtifactComponent', () => {
  let component: SubmitArtifactComponent;
  let fixture: ComponentFixture<SubmitArtifactComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SubmitArtifactComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SubmitArtifactComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});