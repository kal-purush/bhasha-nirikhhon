import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CourseComponent } from './course.component';
import {MdListModule, MdIconModule, MdTooltipModule} from "@angular/material";
import {SharedService} from "../shared.service";
import {RouterTestingModule} from "@angular/router/testing";
import {MessageComponent} from "../message/message.component";
import {FormsModule} from "@angular/forms";

describe('CourseComponent', () => {
  let component: CourseComponent;
  let fixture: ComponentFixture<CourseComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CourseComponent, MessageComponent],
      imports:[MdListModule, RouterTestingModule, MdIconModule, FormsModule, MdTooltipModule],
      providers: [SharedService]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CourseComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});