import { NO_ERRORS_SCHEMA } from '@angular/core'
import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing'
import { HttpClient } from '@angular/common/http'
import { HttpClientTestingModule } from '@angular/common/http/testing'
import { TranslateLoader, TranslateModule } from '@ngx-translate/core'
import { of, throwError } from 'rxjs'
import { FormControl, FormGroup } from '@angular/forms'

import {
  AppStateService,
  createTranslateLoader,
  PortalMessageService,
  UserService
} from '@onecx/portal-integration-angular'
import { ApplicationParameter, ParametersAPIService, ProductStorePageResult } from 'src/app/shared/generated'
import { ParameterDetailComponent } from './parameter-detail.component'

const productName = 'prod1'
const app = 'app1'

const parameter: ApplicationParameter = {
  id: 'id',
  productName: productName,
  applicationId: app,
  key: 'key',
  setValue: 'value'
}

describe('ParameterDetailComponent', () => {
  let component: ParameterDetailComponent
  let fixture: ComponentFixture<ParameterDetailComponent>

  const msgServiceSpy = jasmine.createSpyObj<PortalMessageService>('PortalMessageService', ['success', 'error'])
  const apiServiceSpy = {
    createParameterValue: jasmine.createSpy('createParameterValue').and.returnValue(of({})),
    updateParameterValue: jasmine.createSpy('updateParameterValue').and.returnValue(of({}))
  }
  const formGroup = new FormGroup({
    id: new FormControl('id'),
    key: new FormControl('key'),
    value: new FormControl('value'),
    productName: new FormControl('prod name'),
    applicationId: new FormControl('app')
  })
  const mockUserService = {
    lang$: {
      getValue: jasmine.createSpy('getValue')
    }
  }

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [ParameterDetailComponent],
      imports: [
        HttpClientTestingModule,
        TranslateModule.forRoot({
          loader: {
            provide: TranslateLoader,
            useFactory: createTranslateLoader,
            deps: [HttpClient, AppStateService]
          }
        })
      ],
      schemas: [NO_ERRORS_SCHEMA],
      providers: [
        { provide: PortalMessageService, useValue: msgServiceSpy },
        { provide: ParametersAPIService, useValue: apiServiceSpy },
        { provide: UserService, useValue: mockUserService }
      ]
    }).compileComponents()
    msgServiceSpy.success.calls.reset()
    msgServiceSpy.error.calls.reset()
    apiServiceSpy.createParameterValue.calls.reset()
    apiServiceSpy.updateParameterValue.calls.reset()
  }))

  beforeEach(() => {
    fixture = TestBed.createComponent(ParameterDetailComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  afterEach(() => {
    component.formGroup.reset()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })

  describe('ngOnChange, i.e. opening detail dialog', () => {
    it('should prepare editing an parameter', () => {
      component.changeMode = 'EDIT'
      component.parameter = parameter

      component.ngOnChanges()

      expect(component.parameterId).toEqual(parameter.id)
    })

    it('should prepare copying an parameter', () => {
      component.changeMode = 'NEW'
      component.parameter = parameter
      component.ngOnChanges()

      expect(component.parameterId).toBeUndefined()
    })

    it('should prepare creating an parameter', () => {
      component.changeMode = 'NEW'
      spyOn(component.formGroup, 'reset')

      component.ngOnChanges()

      expect(component.formGroup.reset).toHaveBeenCalled()
    })
  })

  describe('onSave - creating and updating an parameter', () => {
    it('should create an parameter', () => {
      apiServiceSpy.createParameterValue.and.returnValue(of({}))
      component.changeMode = 'NEW'
      spyOn(component.hideDialogAndChanged, 'emit')
      component.formGroup = formGroup

      component.onSave()

      expect(msgServiceSpy.success).toHaveBeenCalledWith({
        summaryKey: 'ACTIONS.CREATE.MESSAGE.OK'
      })
      expect(component.hideDialogAndChanged.emit).toHaveBeenCalledWith(true)
    })

    it('should display error if creation fails', () => {
      apiServiceSpy.createParameterValue.and.returnValue(throwError(() => new Error()))
      component.changeMode = 'NEW'
      component.formGroup = formGroup

      component.onSave()

      expect(component.formGroup.valid).toBeTrue()
      expect(msgServiceSpy.error).toHaveBeenCalledWith({
        summaryKey: 'ACTIONS.CREATE.MESSAGE.NOK'
      })
    })

    it('should update an parameter', () => {
      apiServiceSpy.updateParameterValue.and.returnValue(of({}))
      component.changeMode = 'EDIT'
      spyOn(component.hideDialogAndChanged, 'emit')
      component.parameterId = 'id'
      component.formGroup = formGroup

      component.onSave()

      expect(msgServiceSpy.success).toHaveBeenCalledWith({
        summaryKey: 'ACTIONS.EDIT.MESSAGE.OK'
      })
      expect(component.hideDialogAndChanged.emit).toHaveBeenCalledWith(true)
    })

    it('should display error if update fails', () => {
      apiServiceSpy.updateParameterValue.and.returnValue(throwError(() => new Error()))
      component.changeMode = 'EDIT'
      component.parameterId = 'id'
      component.formGroup = formGroup

      component.onSave()

      expect(msgServiceSpy.error).toHaveBeenCalledWith({
        summaryKey: 'ACTIONS.EDIT.MESSAGE.NOK'
      })
    })
  })

  describe('updateApplicationIds', () => {
    it('should update applicationIds based on the product name', () => {
      component.products = {
        stream: [
          {
            productName: 'Product A',
            applications: ['App1', 'App2']
          },
          {
            productName: 'Product B',
            applications: ['App3']
          }
        ]
      } as ProductStorePageResult

      component.updateApplicationIds('Product A')

      expect(component.applicationIds).toEqual([
        { label: 'App1', value: 'App1' },
        { label: 'App2', value: 'App2' }
      ])
      expect(component.formGroup.controls['applicationId'].value).toBeNull()
    })

    it('should clear applicationIds if productName does not match', () => {
      component.products = {
        stream: [
          {
            productName: 'Product A',
            applications: ['App1', 'App2']
          }
        ]
      } as ProductStorePageResult

      component.updateApplicationIds('Product C')

      expect(component.applicationIds).toEqual([])
      expect(component.formGroup.controls['applicationId'].value).toBeNull()
    })

    it('should handle empty or undefined products', () => {
      component.products = undefined

      component.updateApplicationIds('Product A')

      expect(component.applicationIds).toEqual([])

      expect(component.formGroup.controls['applicationId'].value).toBeNull()
    })
  })

  /*
   * UI ACTIONS
   */
  it('should close the dialog', () => {
    spyOn(component.hideDialogAndChanged, 'emit')
    component.onDialogHide()

    expect(component.displayDetailDialog).toBeFalse()
    expect(component.hideDialogAndChanged.emit).toHaveBeenCalledWith(false)
  })
})