import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TagCardComponent } from './tag-card.component';

describe('TagCardComponent', () => {
  let component: TagCardComponent;
  let fixture: ComponentFixture<TagCardComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TagCardComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TagCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});