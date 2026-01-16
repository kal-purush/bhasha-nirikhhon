import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { DiaryDetailComponent } from './diary-detail.component';
import { DiaryService } from '../shared/diary.service';
import { DiaryListComponent } from '../diary-list/diary-list.component';
import { Diary } from '../shared/diary.model';

describe('DiaryDetailComponent', () => {
  let component: DiaryDetailComponent;
  let fixture: ComponentFixture<DiaryDetailComponent>;
  let diaryServiceStub;

  beforeEach(async(() => {
    diaryServiceStub = {
      getDiaries() {},
      addDiary(diary: Diary) {}
    };
    TestBed.configureTestingModule({
      imports: [ FormsModule, RouterTestingModule ],
      declarations: [ DiaryListComponent, DiaryDetailComponent ],
      providers: [
        {provide: DiaryService, useValue: diaryServiceStub}
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DiaryDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});