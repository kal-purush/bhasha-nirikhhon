import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GetcandidatesComponent } from './getcandidates.component';

describe('GetcandidatesComponent', () => {
  let component: GetcandidatesComponent;
  let fixture: ComponentFixture<GetcandidatesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GetcandidatesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GetcandidatesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});