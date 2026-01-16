import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NewxComponent } from './newx.component';

describe('NewxComponent', () => {
  let component: NewxComponent;
  let fixture: ComponentFixture<NewxComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [NewxComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(NewxComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});