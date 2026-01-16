import { ComponentFixture, TestBed } from '@angular/core/testing';

import { JumbletronDemonstrationComponent } from './jumbletron-demonstration.component';

describe('JumbletronDemonstrationComponent', () => {
  let component: JumbletronDemonstrationComponent;
  let fixture: ComponentFixture<JumbletronDemonstrationComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ JumbletronDemonstrationComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(JumbletronDemonstrationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});