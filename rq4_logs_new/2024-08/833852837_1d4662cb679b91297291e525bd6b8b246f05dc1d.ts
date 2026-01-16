import { ChangeDetectionStrategy, Component, signal } from '@angular/core';
import { FriendlyChatBaseComponent } from '../../friendly-chat-base/friendly-chat-base.component';
import { JsonPipe } from '@angular/common';
import { Course, Lesson } from 'src/app/common/interfaces';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import {MatTableModule} from '@angular/material/table';

@Component({
  selector: 'app-course',
  standalone: true,
  imports: [JsonPipe, MatProgressSpinnerModule, MatTableModule],
  templateUrl: './course.component.html',
  styleUrl: './course.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class CourseComponent extends FriendlyChatBaseComponent {

    course = signal<Course | undefined>(this.friendlyChatStore.selectedCourseWithLessons().selectedCourse);
    
    lessons = signal<Lesson[]>(this.friendlyChatStore.selectedCourseWithLessons().lessonsForCourse || []);

    lastPageLoaded = 0;

    displayedColumns = ['seqNo', 'description', 'duration'];

}