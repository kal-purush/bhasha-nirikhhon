import { Component, OnDestroy, OnInit, ViewChild, ElementRef } from '@angular/core';
import { WorkoutClient, WorkoutDto } from '../../../services/api.service';
import { Router, ActivatedRoute } from '@angular/router';
import { ToastrService } from 'ngx-toastr';

@Component({
  templateUrl: './workout.component.html',
  styleUrls: ['../common.component.css']
})
export class WorkoutComponent implements OnInit, OnDestroy {

  sub: any;
  id: number = 0;
  workout: WorkoutDto = new WorkoutDto();
  @ViewChild('field', { static: false }) field: ElementRef;

  constructor(private workoutClient: WorkoutClient,
    private route: ActivatedRoute,
    private toastr: ToastrService,
    private router: Router,
  ) {
  }
  ngAfterViewInit() {
    setTimeout(() => {
      this.field.nativeElement.focus();
    }, 200);
  }

  ngOnInit() {

    this.sub = this.route.params.subscribe(params => {
      this.id = +params['id'];
      if (this.id > 0) {
        this.loadWorkout();
      }
    });
  }

  loadWorkout() {
    this.workoutClient.get(this.id).subscribe((data) => {
      this.workout = data;
    });
  }

  onSubmit() {

    if (this.workout.startDate)
      this.workout.startDate = new Date(this.workout.startDate);
    if (this.workout.endDate)
      this.workout.endDate = new Date(this.workout.endDate);

    if (this.id > 0) {
      this.workoutClient.update(this.workout).subscribe((data) => {
        this.toastr.success('Save Successful!', '');
        this.router.navigateByUrl('workouts');
      });
    }
    else {
      this.workoutClient.create(this.workout).subscribe((data) => {
        this.toastr.success('Save Successful!', '');
        this.router.navigateByUrl('workouts');
      });
    }
  }

  cancel() {
    this.router.navigateByUrl('workouts');
  }

  ngOnDestroy() {
    this.sub.unsubscribe();
  }
}