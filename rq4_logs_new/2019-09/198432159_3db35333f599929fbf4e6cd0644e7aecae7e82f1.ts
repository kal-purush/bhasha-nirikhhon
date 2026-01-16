import {
  LitElement, html, customElement, property
} from 'lit-element';
import {logError} from '@unicef-polymer/etools-behaviors/etools-logging';
import EtoolsDialog from '@unicef-polymer/etools-dialog/etools-dialog';
import {ConfigObj, createDynamicDialog, removeDialog} from '@unicef-polymer/etools-dialog/dynamic-dialog';
import {Assessment} from '../../../types/assessment';
import {getEndpoint} from '../../../endpoints/endpoints';
import {etoolsEndpoints} from '../../../endpoints/endpoints-list';
import {makeRequest} from '../../utils/request-helper';
import {connect} from 'pwa-helpers/connect-mixin';
import {RootState, store} from '../../../redux/store';
import {updateAssessmentData} from '../../../redux/actions/page-data';
import {parseRequestErrorsAndShowAsToastMsgs} from '../../utils/ajax-error-parser';
import {buttonsStyles} from '../../styles/button-styles';

@customElement('assessment-status-transition-actions')
export class AssessmentStatusTransitionActions extends connect(store)(LitElement) {

  @property({type: Object})
  assessment!: Assessment;

  private statusChangeConfirmationDialog: EtoolsDialog | null = null;
  private confirmationMSg: HTMLSpanElement = document.createElement('span');
  private currentStatusAction = '';

  render() {
    // language=HTML
    return html`
      ${buttonsStyles}
      ${this.cancelAssessmentStatusActionTmpl(this.assessment)}
      ${this.assessmentStatusActionBtnsTmpl(this.assessment)}
    `;
  }

  cancelAssessmentStatusActionTmpl(assessment: Assessment) {
    if (!this.canShowCancelAction(assessment)) {
      return;
    }
    return html`
      <paper-button class="default right-icon" raised @tap="${() => this.updateAssessmentStatus('cancel')}">
        Cancel
        <iron-icon icon="remove-circle-outline"></iron-icon>
      </paper-button>
    `;
  }

  assessmentStatusActionBtnsTmpl(assessment: Assessment) {
    if (!this.canShowStatusActions(assessment)) {
      return;
    }
    switch (assessment.status) {
      case 'draft':
        return html`
        <paper-button class="primary right-icon" raised @tap="${() => this.updateAssessmentStatus('assign')}">
          Assign
          <iron-icon icon="assignment-ind"></iron-icon>
        </paper-button>
      `;
      case 'in_progress':
        return html`
        <paper-button class="primary right-icon" raised @tap="${() => this.updateAssessmentStatus('submit')}">
          Submit
          <iron-icon icon="chevron-right"></iron-icon>
        </paper-button>
      `;
      case 'submitted':
        return html`
        <paper-button class="error right-icon" raised @tap="${() => this.updateAssessmentStatus('reject')}">
          Reject
          <iron-icon icon="assignment-return"></iron-icon>
        </paper-button>
        <paper-button class="success right-icon" raised @tap="${() => this.updateAssessmentStatus('finalize')}">
          Finalize
          <iron-icon icon="chevron-right"></iron-icon>
        </paper-button>
      `;
      default:
        return '';
    }
  }

  connectedCallback(): void {
    super.connectedCallback();
    this.createStatusChangeConfirmationsDialog();
  }

  disconnectedCallback(): void {
    super.disconnectedCallback();
    this.removeStatusChangeConfirmationsDialog();
  }

  public stateChanged(state: RootState) {
    if (state.pageData!.currentAssessment) {
      // initialize assessment object
      this.assessment = {...state.pageData!.currentAssessment};
    }
  }

  canShowCancelAction(assessment: Assessment): boolean {
    // TODO: include user group validation here, only for unicef users/unicef focal point can cancel
    return assessment && !!assessment.id && ['submitted', 'final'].indexOf(this.assessment.status) === -1;
  }

  canShowStatusActions(assessment: Assessment) {
    return assessment && assessment.id && !!assessment.assessor;
  }

  validateStatusChange(): boolean {
    let valid = false;
    switch (this.currentStatusAction) {
      case 'assign':
        // assessment should be added
        valid = this.assessment !== null && !!this.assessment.id && !!this.assessment.assessor;
        break;
      case 'submit':
      case 'reject':
      case 'finalize':
        // use only validation of status transition of the API
        valid = true;
        break;
      case 'cancel':
        // TODO: determine cancel validations by user group and add it to this condition
        valid = this.assessment !== null && this.canShowCancelAction(this.assessment);
        break;
    }
    return valid;
  }

  updateAssessmentStatus(action: string) {
    this.currentStatusAction = action;
    if (!this.validateStatusChange()) {
      // TODO: show a toast message explaining why status change cannot be made
      this.currentStatusAction = '';
      return;
    }

    this.updateConfirmationMsgAction(this.currentStatusAction);
    if (!this.statusChangeConfirmationDialog) {
      throw new Error('statusChangeConfirmationDialog is not created!');
    }
    this.statusChangeConfirmationDialog.opened = true;
  }

  updateConfirmationMsgAction(action: string) {
    this.confirmationMSg.innerText = `Are you sure you want to ${action} this assessment`;
  }

  onStatusChangeConfirmation(e: CustomEvent) {
    if (!e.detail.confirmed) {
      // cancel status update action
      this.currentStatusAction = '';
      return;
    }
    if (!this.assessment || !this.currentStatusAction) {
      throw new Error('Assessment obj or statusAction not set!');
    }
    const url = getEndpoint(etoolsEndpoints.assessmentStatusUpdate,
      {id: this.assessment.id, statusAction: this.currentStatusAction}).url!;
    console.log('update status: PATCH', url);
    return makeRequest({url: url, method: 'PATCH'})
      .then((response) => {
        // update assessment data in redux store
        const updatedAssessment = Object.assign({}, this.assessment, {status: response.status});
        console.log('success on status change...', response, updatedAssessment);
        store.dispatch(updateAssessmentData(updatedAssessment));
      })
      .catch((err: any) => {
        logError(err);
        parseRequestErrorsAndShowAsToastMsgs(err, this);
      }).then(() => {
        // req finalized... reset data
        this.currentStatusAction = '';
      });
  }

  createStatusChangeConfirmationsDialog() {
    if (!this.statusChangeConfirmationDialog) {
      this.onStatusChangeConfirmation = this.onStatusChangeConfirmation.bind(this);
      const confirmationDialogConf: ConfigObj = {
        title: 'Assessment status update',
        size: 'md',
        okBtnText: 'Yes',
        cancelBtnText: 'No',
        closeCallback: this.onStatusChangeConfirmation,
        content: this.confirmationMSg
      };
      this.statusChangeConfirmationDialog = createDynamicDialog(confirmationDialogConf);
      this.statusChangeConfirmationDialog.updateStyles({'--etools-dialog-confirm-btn-bg': 'var(--primary-color)'});
    }
  }

  removeStatusChangeConfirmationsDialog() {
    if (this.statusChangeConfirmationDialog !== null) {
      removeDialog(this.statusChangeConfirmationDialog);
    }
  }
}

