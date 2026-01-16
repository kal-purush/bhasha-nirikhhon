import { Component, OnInit, inject, output } from '@angular/core';
import {
  FormControl,
  FormGroup,
  ReactiveFormsModule,
  Validators,
} from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatIconModule } from '@angular/material/icon';
import { PollService } from '../../../../core/services/api/poll.service';
import { CohortService } from '../../../../core/services/api/cohort.service';
import { PollModel } from '../../../../core/models/poll.model';
import { CohortModel } from '../../../../core/models/cohort.model';
import { VariableModel } from '../../../../core/models/variable.model';
import { debounceTime } from 'rxjs';

@Component({
  selector: 'app-poll-filters',
  imports: [
    ReactiveFormsModule,
    MatFormFieldModule,
    MatSelectModule,
    MatIconModule,
  ],
  templateUrl: './poll-filters.component.html',
  styleUrl: './poll-filters.component.css',
})
export class PollFiltersComponent implements OnInit {
  pollsService = inject(PollService);
  cohortsService = inject(CohortService);
  polls: PollModel[] = [];
  cohorts: CohortModel[] = [];
  componentNames: string[] = [];
  variables: VariableModel[] = [];

  filters = output<{
    title: string;
    uuid: string;
    cohortId: number;
    variableIds: number[];
  }>();

  filterForm = new FormGroup({
    pollUuid: new FormControl<string | null>(null, [Validators.required]),
    cohortId: new FormControl<number | null>(null, [Validators.required]),
    components: new FormControl<string[] | null>([], [Validators.required]),
    variables: new FormControl<number[]>([], [Validators.required]),
  });

  selectAllCohort: CohortModel = {
    id: 0,
    name: 'Select All',
    courseCode: 'Select All',
  };

  ngOnInit(): void {
    this.pollsService.getAllPolls().subscribe(res => (this.polls = res));
    this.cohortsService
      .getCohorts()
      .subscribe(res => (this.cohorts = [this.selectAllCohort, ...res.body]));

    this.filterForm.controls.cohortId.valueChanges.subscribe(() => {
      this.handleCohortSelect();
    });
    this.filterForm.controls.components.valueChanges
      .pipe(debounceTime(300))
      .subscribe(() => this.handleUpdateComponents());
  }

  handleCohortSelect() {
    if (!this.filterForm.value.pollUuid) return;
    this.pollsService
      .getVariablesByComponents(this.filterForm.value.pollUuid, [
        ...this.componentNames,
      ])
      .subscribe(variables => {
        this.variables = variables;
        this.componentNames = [...new Set(variables.map(v => v.componentName))];
        this.filterForm.controls.components.setValue(this.componentNames);
        this.filterForm.controls.variables.setValue(
          this.variables.map(v => v.id)
        );
        this.sendFilters();
      });
  }

  handleUpdateComponents() {
    if (
      !this.filterForm.value.pollUuid ||
      this.filterForm.value.cohortId === null
    )
      return;
    if (this.filterForm.value.components!.length === 0) {
      this.filterForm.controls.variables.setValue([]);
      return;
    }
    this.filterForm.controls.variables.setValue(
      this.variables
        .filter(v =>
          this.filterForm.value.components?.includes(v.componentName)
        )
        .map(v => v.id)
    );
  }

  onOpenedChange(isOpen: boolean) {
    if (!isOpen) this.sendFilters();
  }
  sendFilters() {
    const pollName = this.polls.find(
      p => p.uuid === this.filterForm.value.pollUuid
    )?.name;
    const cohortName =
      this.filterForm.value.cohortId === 0
        ? 'All cohorts'
        : this.cohorts.find(c => c.id === this.filterForm.value.cohortId)?.name;
    const title = `Poll: ${pollName} - Cohort: ${cohortName}`;
    const uuid = this.filterForm.value.pollUuid;
    const cohortId = this.filterForm.value.cohortId;
    const variableIds = this.filterForm.value.variables;
    if (!uuid || cohortId == null || !variableIds) return;
    this.filters.emit({ title, uuid, cohortId, variableIds });
  }
}