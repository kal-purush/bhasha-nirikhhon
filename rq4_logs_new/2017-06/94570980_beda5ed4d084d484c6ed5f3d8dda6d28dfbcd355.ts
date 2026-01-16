import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ChartsModule } from '../charts/charts.module';

import { IntroComponent } from './intro/intro.component';
import { PipelineRunByStageComponent } from './pipeline-run-by-stage/pipeline-run-by-stage.component';
import { TestCaseStatusByPipelineRunComponent } from './test-case-status-by-pipeline-run/test-case-status-by-pipeline-run.component';
import { TestCaseStatusByTestSuiteComponent } from './test-case-status-by-test-suite/test-case-status-by-test-suite.component';
import { TestCaseStatusByTestSuiteBasedOnBuildPackageComponent } from './test-case-status-by-test-suite-based-on-build-package/test-case-status-by-test-suite-based-on-build-package.component';
import { TestSuiteBuildsByTimeComponent } from './test-suite-builds-by-time/test-suite-builds-by-time.component';

@NgModule({
  imports: [
    CommonModule,
    ChartsModule
  ],
  declarations: [
    IntroComponent,
    PipelineRunByStageComponent,
    TestCaseStatusByPipelineRunComponent,
    TestCaseStatusByTestSuiteComponent,
    TestCaseStatusByTestSuiteBasedOnBuildPackageComponent,
    TestSuiteBuildsByTimeComponent
  ],
  exports: [
    IntroComponent,
    PipelineRunByStageComponent,
    TestCaseStatusByPipelineRunComponent,
    TestCaseStatusByTestSuiteComponent,
    TestCaseStatusByTestSuiteBasedOnBuildPackageComponent,
    TestSuiteBuildsByTimeComponent
  ]
})
export class SectionsModule { }