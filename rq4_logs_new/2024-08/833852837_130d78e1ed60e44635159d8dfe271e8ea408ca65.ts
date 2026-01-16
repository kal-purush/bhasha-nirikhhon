import { ChangeDetectionStrategy, Component, inject, signal } from '@angular/core';
import { Course, Lesson } from 'src/app/common/interfaces';
import { COURSES, LESSONS } from 'src/app/common/mock-data';
import { CoursesService } from 'src/app/services/courses.service';

@Component({
  selector: 'app-friendly-chat-base',
  standalone: true,
  imports: [],
  template: ``,
  styles: ``,
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class FriendlyChatBaseComponent {

    coursesService = inject(CoursesService);

    
    courses = signal<Course[]>(COURSES);
    lessons = signal<Lesson[]>(LESSONS);

}