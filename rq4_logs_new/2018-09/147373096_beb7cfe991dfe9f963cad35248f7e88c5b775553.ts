import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { DynaFormComponent } from './dyna-form.component';

describe('FormBuilderComponent', () => {
  let component: DynaFormComponent;
  let fixture: ComponentFixture<DynaFormComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [DynaFormComponent],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DynaFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});