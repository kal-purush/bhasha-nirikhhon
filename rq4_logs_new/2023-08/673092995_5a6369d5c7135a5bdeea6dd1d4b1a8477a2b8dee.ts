import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RecordTableComponent } from './record-table.component';

describe('RecordTableComponent', () => {
  let component: RecordTableComponent;
  let fixture: ComponentFixture<RecordTableComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [RecordTableComponent]
    });
    fixture = TestBed.createComponent(RecordTableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});