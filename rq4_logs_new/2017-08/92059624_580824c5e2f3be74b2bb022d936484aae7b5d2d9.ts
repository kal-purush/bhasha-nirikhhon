import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BrowsePackageComponent } from './browse-package.component';

describe('BrowsePackageComponent', () => {
  let component: BrowsePackageComponent;
  let fixture: ComponentFixture<BrowsePackageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BrowsePackageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BrowsePackageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});