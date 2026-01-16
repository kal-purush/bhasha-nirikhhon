import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AddCurrentAssetComponent } from './add-current-asset.component';

describe('AddCurrentAssetComponent', () => {
  let component: AddCurrentAssetComponent;
  let fixture: ComponentFixture<AddCurrentAssetComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AddCurrentAssetComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AddCurrentAssetComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});