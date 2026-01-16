import {LitElement, html, property, customElement} from 'lit-element';
import {PolymerElement} from '@polymer/polymer/polymer-element.js';
import '@polymer/paper-input/paper-textarea.js';
import {GenericObject, ActionPoint} from '../../../../../types/globals';
import {Assessment} from '../../../../../types/assessment';
import '@unicef-polymer/etools-dialog/etools-dialog.js';
import '@unicef-polymer/etools-dropdown/etools-dropdown.js';
import '@unicef-polymer/etools-date-time/datepicker-lite.js';
import {logError} from '@unicef-polymer/etools-behaviors/etools-logging';
import {fireEvent} from '../../../../utils/fire-custom-event';
import {getEndpoint} from '../../../../../endpoints/endpoints';
import {makeRequest} from '../../../../utils/request-helper';
import {connect} from 'pwa-helpers/connect-mixin.js';
import {store, RootState} from '../../../../../redux/store';
import {etoolsEndpoints} from '../../../../../endpoints/endpoints-list';
import {cloneDeep, isJsonStrMatch} from '../../../../utils/utils';
import {formatServerErrorAsText} from '../../../../utils/ajax-error-parser';
import {SharedStylesLit} from '../../../../styles/shared-styles-lit';
import {gridLayoutStylesLit} from '../../../../styles/grid-layout-styles-lit';
import isEqual from 'lodash-es/isEqual';

@customElement('follow-up-dialog')
export class FollowUpDialog extends connect(store)(LitElement) {
  render() {
    return html`
      ${gridLayoutStylesLit}
      ${SharedStylesLit}
      <style>
        :host .copy-warning {
          position: relative;
          margin-bottom: 10px;
          padding: 20px 24px;
          background-color: var(--secondary-background-color);
          font-size: 15px;
        }

        etools-dropdown {
          --esmm-external-wrapper: {
            width: 100%;
          };
        }

        paper-textarea {
          width: 100%;
        }

        paper-checkbox {
          margin-top: 20px;
        }
      </style>

      <etools-dialog keep-dialog-open size="md"
                     ?opened="${this.dialogOpened}"
                     dialog-title="${this.dialogTitle}"
                     ok-btn-text="${this.confirmBtnTxt}"
                     ?hide-confirm-btn="${!this.confirmBtnTxt}"
                     ?show-spinner="${this.requestInProcess}"
                     ?disable-confirm-btn="${this.requestInProcess}"
                     @confirm-btn-clicked="${this.onConfirmBtnClick}"
                     @close="${this.handleDialogClosed}">
        <!-- TODO: The following warning may be replaced -->
        ${this.watchForChanges ? html`
            <div class="copy-warning">
                It is required to change at least one of the fields below.
            </div>` : ''}

        <div class="layout-horizontal">
          <div class="col col-6">
            <etools-dropdown
                    id="partnerInput"
                    required
                    .selected="${this.editedItem.partner}"
                    label="Partner"
                    .options="${this.partners}"
                    option-label="name"
                    option-value="id"
                    ?readOnly="${this.editedItem.partner}">
            </etools-dropdown>
          </div>
          <div class="col col-6">
            <etools-dropdown
                    id="assessmentInput"
                    required
                    .selected="${this.assessment.id}"
                    label="Assessment"
                    .options="${[this.assessment]}"
                    option-label="reference_number"
                    option-value="id"
                    ?readOnly="${this.assessment.id}">
            </etools-dropdown>
          </div>
        </div>

        <!-- <div class="layout-horizontal">
          <div class="col col-6">
            <etools-dropdown
                    id="categoryInput"
                    // @ts-ignore
                    .selected="${this.editedItem.category}"
                    label="Category"
                    .options="${this.categories}"
                    option-label="display_name"
                    required
                    option-value="value">
            </etools-dropdown>
          </div>
        </div> -->

        <div class="layout-horizontal">
          <paper-textarea
                  id="descriptionInput"
                  required
                  allowed-pattern="[\d\s]"
                  value="${this.editedItem.description}"
                  label="Description"
                  max-rows="4">
          </paper-textarea>
        </div>

        <div class="layout-horizontal">
          <div class="col col-6">
            <etools-dropdown
                    id="assignedToInput"
                    .selected="${this.editedItem.assigned_to}"
                    label="Assigned To"
                    .options="${this.users}"
                    option-label="name"
                    required
                    option-value="id">
            </etools-dropdown>
          </div>

          <div class="col col-6">
            <etools-dropdown
                    id="sectionInput"
                    .selected="${this.editedItem.section}"
                    label="Section"
                    .options="${this.sections}"
                    option-label="name"
                    required
                    option-value="id">
            </etools-dropdown>
          </div>
        </div>

        <div class="layout-horizontal">
          <div class="col col-6">
            <etools-dropdown
                    id="officeInput"
                    .selected="${this.editedItem.office}"
                    label="Office"
                    .options="${this.offices}"
                    option-label="name"
                    required
                    option-value="id">
            </etools-dropdown>
          </div>

          <div class="col col-6">
            <datepicker-lite
                    id="dueDateInput"
                    value="${this.editedItem.due_date}"
                    label="Due Date"
                    required
                    selected-date-display-format="D MMM YYYY">
            </datepicker-lite>
          </div>
        </div>

        <div class="layout-horizontal">
          <paper-checkbox
                  id="highPriorityInput"
                  ?checked="${this.editedItem.high_priority}">
                  This action point is high priority
          </paper-checkbox>
        </div>
      </etools-dialog>
    `;
  }

  private defaultItem: ActionPoint = {
    partner: null,
    id: null,
    psea_assessment: null,
    assigned_to: null,
    section: null,
    office: null,
    description: '',
    due_date: '',
    high_priority: false
  };

  private validationSelectors: string[] = ['#categoryInput', '#assignedToInput',
    '#sectionInput', '#officeInput', '#dateInput'];

  @property({type: Array})
  users: GenericObject[] = [];

  @property({type: Array})
  sections: GenericObject[] = [];

  @property({type: Array})
  offices: GenericObject[] = [];

  @property({type: Boolean})
  dialogOpened: boolean = false;

  @property({type: Array})
  partners: object[] = [];

  @property({type: Object})
  assessment!: Assessment;

  @property({type: String})
  dialogTitle: string = 'Add Action Point';

  @property({type: String})
  confirmBtnTxt: string = 'Save';

  @property({type: String})
  cancelBtnText: string = 'Cancel';

  @property({type: Object})
  editedItem!: ActionPoint;

  @property({type: Array})
  categories: GenericObject[] = [];

  @property({type: Boolean})
  requestInProcess: boolean = false;

  @property({type: Boolean})
  isNewRecord: boolean = true;

  @property({type: Object})
  toastEventSource!: LitElement;

  @property({type: Boolean})
  watchForChanges: boolean = false;

  stateChanged(state: RootState) {
    if (state.commonData) {
      if (!isJsonStrMatch(this.partners, state.commonData.partners)) {
        this.partners = [...state.commonData.partners];
      }
      if (!isJsonStrMatch(this.users, state.commonData.unicefUsers)) {
        this.users = [...state.commonData.unicefUsers];
      }
      if (!isJsonStrMatch(this.offices, state.commonData.offices)) {
        this.offices = [...state.commonData.offices];
      }
      if (!isJsonStrMatch(this.sections, state.commonData.sections)) {
        this.sections = [...state.commonData.sections];
      }
    }

    if (!isJsonStrMatch(this.assessment, state.pageData!.currentAssessment)) {
      // initialize assessment object
      this.assessment = cloneDeep(state.pageData!.currentAssessment);
      this.resetEditedItem();
    }
  }

  updated(changedProperties: GenericObject) {
    if (this.watchForChanges && !changedProperties.has('watchForChanges') &&
        !isEqual(this.editedItem, changedProperties.get('editedItem'))) {
      this.watchForChanges = !this.watchForChanges;
    }
  }

  onConfirmBtnClick() {
    if (this.validate()) {
      this.saveDialogData();
    }
  }

  private validate() {
    let isValid = true;
    this.validationSelectors.forEach((selector: string) => {
      const el = this.shadowRoot!.querySelector(selector) as PolymerElement & { validate(): boolean };
      if (el && !el.validate()) {
        isValid = false;
      }
    });
    return isValid;
  }

  private getControlsData() {
    // this.editedItem.category = this.getEl('#categoryInput').selected;
    // @ts-ignore
    this.editedItem.assigned_to = this.getEl('#assignedToInput').selected;
    // @ts-ignore
    this.editedItem.section = this.getEl('#sectionInput').selected;
    // @ts-ignore
    this.editedItem.office = this.getEl('#officeInput').selected;
    this.editedItem.due_date = this.getEl('#dueDateInput').value;
    this.editedItem.description = this.getEl('#descriptionInput').value;
    this.editedItem.high_priority = this.getEl('#highPriorityInput').checked;
  }

  private saveDialogData() {
    this.getControlsData();
    this.requestInProcess = true;
    const options: any = {
      method: this.isNewRecord ? 'POST' : 'PATCH',
      url: this.isNewRecord ?
        getEndpoint(etoolsEndpoints.actionPoints, {id: this.assessment.id}).url :
        getEndpoint(etoolsEndpoints.editActionPoint, {id: this.assessment.id, actionPoint: this.editedItem.id}).url
    };

    makeRequest(options, this.editedItem)
      .then((resp: any) => this._handleResponse(resp))
      .catch((err: any) => this._handleError(err));
  }

  _handleResponse(resp: any) {
    this.requestInProcess = false;
    fireEvent(this, 'action-point-updated', {...resp});
    this.handleDialogClosed();
  }

  _handleError(err: any) {
    this.requestInProcess = false;
    const msg = 'Failed to save/update new Action Point!';
    logError(msg, 'action-point', err);
    fireEvent(this.toastEventSource, 'toast', {text: formatServerErrorAsText(err)});
  }

  private handleDialogClosed() {
    this.dialogOpened = false;
  }

  resetEditedItem() {
    this.editedItem = cloneDeep(this.defaultItem);
    this.editedItem.psea_assessment = this.assessment.id;
    // @ts-ignore
    this.editedItem.partner = this.assessment.partner;
  }

  public openDialog() {
    this.isNewRecord = !this.editedItem.id || this.editedItem.id == 'new';
    if (this.isNewRecord) {
      this.resetEditedItem();
    }
    this.dialogTitle = this.isNewRecord ? 'Add Action Point' : 'Edit Action Point';
    this.confirmBtnTxt = this.isNewRecord ? 'Add' : 'Save';
    this.dialogOpened = true;
  }

  getEl(elName: string): HTMLInputElement {
    return this.shadowRoot!.querySelector(elName)! as HTMLInputElement;
  }
}