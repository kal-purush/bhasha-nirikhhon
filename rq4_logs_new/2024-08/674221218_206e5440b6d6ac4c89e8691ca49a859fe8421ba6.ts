import { Component, EventEmitter, Input, OnChanges, Output } from '@angular/core'
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms'
import { TranslateService } from '@ngx-translate/core'
import { SelectItem } from 'primeng/api'

import { PortalMessageService } from '@onecx/portal-integration-angular'
import { ApplicationParameter, ParametersAPIService, ProductStorePageResult } from 'src/app/shared/generated'

@Component({
  selector: 'app-parameter-detail',
  templateUrl: './parameter-detail.component.html',
  styleUrls: ['./parameter-detail.component.scss']
})
export class ParameterDetailComponent implements OnChanges {
  @Input() public changeMode = 'NEW'
  @Input() public displayDetailDialog = false
  @Input() public parameter: ApplicationParameter | undefined
  @Input() public products: ProductStorePageResult | undefined
  @Input() public allProducts: SelectItem[] = []
  @Output() public hideDialogAndChanged = new EventEmitter<boolean>()

  parameterId: string | undefined
  parameterDeleteVisible = false
  public applicationIds: SelectItem[] = []
  public isLoading = false
  // form
  public formGroup: FormGroup

  constructor(
    private parameterApi: ParametersAPIService,
    private fb: FormBuilder,
    private translate: TranslateService,
    private msgService: PortalMessageService
  ) {
    this.formGroup = fb.nonNullable.group({
      productName: new FormControl(null, [Validators.required]),
      applicationId: new FormControl(null, [Validators.required]),
      key: new FormControl(null, [Validators.required]),
      value: new FormControl(null, [Validators.required]),
      description: new FormControl(null, [Validators.required]),
      unit: new FormControl(null),
      rangeFrom: new FormControl(null),
      rangeTo: new FormControl(null)
    })
  }

  ngOnChanges() {
    this.applicationIds = []
    if (this.changeMode === 'EDIT') {
      this.fillForm()
      this.parameterId = this.parameter?.id
    }
    if (this.changeMode === 'NEW') {
      this.parameterId = undefined
      if (this.parameter) {
        this.fillForm() // on COPY
      } else {
        this.formGroup.reset()
      }
    }
  }

  private fillForm(): void {
    this.formGroup.patchValue({
      ...this.parameter
    })
    if (this.changeMode === 'EDIT' || this.changeMode === 'VIEW') {
      this.formGroup.controls['key'].disable()
    } else {
      this.formGroup.controls['key'].enable()
    }
    this.formGroup.controls['value'].setValue(this.parameter?.setValue)
    this.applicationIds.push({
      label: this.parameter?.applicationId,
      value: this.parameter?.applicationId
    })
  }

  public updateApplicationIds(productName: string) {
    this.applicationIds = []
    this.formGroup.controls['applicationId'].reset()
    if (this.products) {
      this.products.stream?.forEach((p) => {
        if (p.productName === productName && p.applications) {
          p.applications.forEach((app) => {
            this.applicationIds.push({
              label: app,
              value: app
            })
          })
          this.formGroup.controls['applicationId'].enable()
        }
      })
    }
  }

  public onDialogHide() {
    this.displayDetailDialog = false
    this.hideDialogAndChanged.emit(false)
  }

  /**
   * SAVING => create or update
   */
  public onSave(): void {
    if (this.formGroup.valid) {
      if (this.changeMode === 'EDIT' && this.parameterId) {
        this.parameterApi
          .updateParameterValue({
            id: this.parameterId,
            applicationParameterUpdate: this.submitFormValues()
          })
          .subscribe({
            next: () => {
              this.msgService.success({ summaryKey: 'ACTIONS.EDIT.MESSAGE.OK' })
              this.hideDialogAndChanged.emit(true)
            },
            error: () => this.msgService.error({ summaryKey: 'ACTIONS.EDIT.MESSAGE.NOK' })
          })
      } else if (this.changeMode === 'NEW') {
        this.parameterApi
          .createParameterValue({
            applicationParameterCreate: this.submitFormValues()
          })
          .subscribe({
            next: () => {
              this.msgService.success({ summaryKey: 'ACTIONS.CREATE.MESSAGE.OK' })
              this.hideDialogAndChanged.emit(true)
            },
            error: () => this.msgService.error({ summaryKey: 'ACTIONS.CREATE.MESSAGE.NOK' })
          })
      }
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private submitFormValues(): any {
    const parameter: ApplicationParameter = { ...this.formGroup.value }
    return parameter
  }
}