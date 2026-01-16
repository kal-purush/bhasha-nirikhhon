import { ChangeDetectionStrategy, Component } from '@angular/core';

@Component({
  selector: 'app-create-course',
  standalone: true,
  imports: [],
  templateUrl: './create-course.component.html',
  styleUrl: './create-course.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class CreateCourseComponent {

}