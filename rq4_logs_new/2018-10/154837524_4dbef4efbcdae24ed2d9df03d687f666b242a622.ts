import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PacketChooserComponent } from './packet-chooser.component';

describe('PacketChooserComponent', () => {
  let component: PacketChooserComponent;
  let fixture: ComponentFixture<PacketChooserComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PacketChooserComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PacketChooserComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});