import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FileLoadingComponent } from './file-loading.component';

describe('FileLoadingComponent', () => {
  let component: FileLoadingComponent;
  let fixture: ComponentFixture<FileLoadingComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FileLoadingComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FileLoadingComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});