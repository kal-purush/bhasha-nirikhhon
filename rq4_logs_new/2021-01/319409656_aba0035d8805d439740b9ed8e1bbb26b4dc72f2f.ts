import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ChiefEditorPubReqListComponent } from './chief-editor-pub-req-list.component';

describe('ChiefEditorPubReqListComponent', () => {
  let component: ChiefEditorPubReqListComponent;
  let fixture: ComponentFixture<ChiefEditorPubReqListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ChiefEditorPubReqListComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ChiefEditorPubReqListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});