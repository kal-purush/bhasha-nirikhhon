import { ComponentFixture, TestBed } from '@angular/core/testing';

import { WebAppNgUtilsComponent } from './web-app-ng-utils.component';

describe('WebAppNgUtilsComponent', () => {
  let component: WebAppNgUtilsComponent;
  let fixture: ComponentFixture<WebAppNgUtilsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ WebAppNgUtilsComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(WebAppNgUtilsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});