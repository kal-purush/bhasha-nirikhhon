/*
Licensed to Gisaïa under one or more contributor
license agreements. See the NOTICE.txt file distributed with
this work for additional information regarding copyright
ownership. Gisaïa licenses this file to you under
the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
import { Component, OnInit, AfterContentChecked, ChangeDetectorRef, ViewChild } from '@angular/core';
import { FormArray, FormGroup } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { CanComponentExit } from '@guards/confirm-exit/confirm-exit.guard';
import { MainFormService } from '@services/main-form/main-form.service';
import { NGXLogger } from 'ngx-logger';
import { ConfigFormGroupComponent } from '@shared-components/config-form-group/config-form-group.component';
import {
  MapVisualisationFormGroup,
  MapVisualisationFormBuilderService
} from '@map-config/services/map-visualisation-form-builder/map-visualisation-form-builder.service';

@Component({
  selector: 'app-edit-visualisation',
  templateUrl: './edit-visualisation.component.html',
  styleUrls: ['./edit-visualisation.component.scss']
})
export class EditVisualisationComponent implements OnInit, CanComponentExit, AfterContentChecked {

  private layersFa: FormArray;
  private visualisationsFa: FormArray;
  private visualisationsValues: any[] = [];
  public forceCanExit: boolean;
  public visualisationFg: MapVisualisationFormGroup;

  @ViewChild(ConfigFormGroupComponent, { static: false }) private configFormGroupComponent: ConfigFormGroupComponent;

  constructor(
    protected mapVisualisationFormBuilder: MapVisualisationFormBuilderService,
    private mainFormService: MainFormService,
    private route: ActivatedRoute,
    private cdref: ChangeDetectorRef,
    private router: Router,
    private logger: NGXLogger) {
    this.visualisationFg = mapVisualisationFormBuilder.buildVisualisation();
  }

  public ngOnInit() {

    this.layersFa = this.mainFormService.mapConfig.getLayersFa();
    this.visualisationsFa = this.mainFormService.mapConfig.getVisualisationsFa();

    if (this.visualisationsFa == null) {
      this.logger.error('Error initializing the page, \'Visualisations\' form group is missing');
      this.navigateToParentPage();
    } else {

      this.visualisationsValues = this.visualisationsFa.value as any[];
      this.route.paramMap.subscribe(params => {
        const visualisationId = params.get('id');
        if (visualisationId != null) {
          // there we are editing an existing visualisation
          const visualisationIndex = this.getVisualisationIndex(Number(visualisationId));
          if (visualisationIndex >= 0) {
            // cannot simply update the existing form instance because we want to allow cancellation
            // so we rather propagate the existing form properties
            const existingVisualisationFg = this.getVisualisationAt(visualisationIndex) as MapVisualisationFormGroup;
            this.visualisationFg.patchValue(existingVisualisationFg.value);
          } else {
            this.navigateToParentPage();
            this.logger.error('Unknown visualisation ID');
          }
        }
      });
    }
  }

  private navigateToParentPage() {
    this.router.navigate(['', 'map-config', 'visualisations'], { queryParamsHandling: 'preserve' });
  }

  public submit() {

    this.visualisationFg.markAllAsTouched();

    // force validation check on mode subform
    if (!this.visualisationFg.valid) {
      this.logger.warn('validation failed', this.visualisationFg);
      return;
    }

    if (!this.isNewVisualisation()) {
      const visualisationIndex = this.getVisualisationIndex(this.visualisationFg.customControls.id.value);
      if (visualisationIndex < 0) {
        this.logger.error('There was an error while saving the visualisation, unknown visualisation ID');
      }
      this.visualisationsFa.setControl(visualisationIndex, this.visualisationFg);
    } else {
      const newId = this.visualisationsValues.reduce((acc, val) => acc.id > val.id ? acc.id : val.id, 0) + 1;
      this.visualisationFg.customControls.id.setValue(newId);
      this.visualisationsFa.insert(newId, this.visualisationFg);
    }

    this.visualisationFg.markAsPristine();
    this.navigateToParentPage();
  }

  private getVisualisationIndex(id: number) {
    return this.visualisationsValues.findIndex(el => el.id === id);
  }

  private getVisualisationAt(index: number) {
    return (this.visualisationsFa.at(index) as FormGroup);
  }

  public isNewVisualisation(): boolean {
    return this.visualisationFg.get('id').value === '';
  }

  public canExit() {
    return this.forceCanExit || this.visualisationFg.pristine;
  }

  public ngAfterContentChecked() {
    // fix ExpressionChangedAfterItHasBeenCheckedError
    this.cdref.detectChanges();
  }

}