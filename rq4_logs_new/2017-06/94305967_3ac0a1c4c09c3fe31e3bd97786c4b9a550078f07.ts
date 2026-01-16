import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BucketlistItemsComponent } from './bucketlist-items.component';

describe('BucketlistItemsComponent', () => {
  let component: BucketlistItemsComponent;
  let fixture: ComponentFixture<BucketlistItemsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BucketlistItemsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BucketlistItemsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});