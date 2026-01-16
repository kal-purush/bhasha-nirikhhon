import {LitElement, html, property} from 'lit-element';
import '@polymer/paper-input/paper-textarea';
import '@polymer/paper-checkbox/paper-checkbox';
import '@polymer/paper-input/paper-input';
import '@polymer/paper-radio-group';
import {gridLayoutStylesLit} from '../../../../styles/grid-layout-styles-lit';
import {labelAndvalueStylesLit} from '../../../../styles/label-and-value-styles-lit';
import {buttonsStyles} from '../../../../styles/button-styles';
import './question-attachments';
import {SharedStylesLit} from '../../../../styles/shared-styles-lit';
import {radioButtonStyles} from '../../../../styles/radio-button-styles';
import {PaperCheckboxElement} from '@polymer/paper-checkbox/paper-checkbox';
import {Answer, Question, ProofOfEvidence, Rating} from '../../../../../types/engagement';

class QuestionnaireAnswer extends LitElement {
  render() {
    return html`
      ${SharedStylesLit}${gridLayoutStylesLit}${labelAndvalueStylesLit}${buttonsStyles}
      ${radioButtonStyles}
      <style>
        .padd-right {
          padding-right: 24px;
        }
        .move-left {
          margin-left: -12px;
        }
        paper-checkbox {
          padding-top: 6px;
          padding-bottom: 8px;
        }
        .extra-padd {
          padding-top: 32px;
        }
      </style>
      <div class="layout-vertical row-padding-v">
        <label class="paper-label">Rating</label>
        <paper-radio-group selected="${this.answer.rating}">
          ${this._getRatingRadioButtonsTemplate(this.question)}
        </paper-radio-group>
      </div>
      <paper-textarea label="Comments" always-float-label class="row-padding-v" .value="${this.answer.comments}">
      </paper-textarea>
      <div class="layout-vertical row-padding-v">
        <label class="paper-label">Proof of Evidence</label>
      </div>

      ${this._getProofOfEvidenceTemplate(this.question.evidences, this.answer)}

      <div class="row-padding-v" ?hidden=${!this.showOtherInput}>
        <paper-input label="Please specify other" always-float-label></paper-input>
      </div>

      <div class="row-padding-v extra-padd">
        <question-attachments>
        </question-attachments>
      </div>

      <div class="layout-horizontal right-align row-padding-v">
      <paper-button class="default">
        Cancel
      </paper-button>
      <paper-button class="primary">
        Save
      </paper-button>
    </div>
    `;
  }

  @property({type: Object})
  question!: Question;

  @property({type: Object})
  answer = new Answer();

  @property({type: Boolean})
  showOtherInput: boolean = false;

  _getProofOfEvidenceTemplate(evidences: ProofOfEvidence[], answer: Answer) {
    return evidences.map((evidence: ProofOfEvidence) => {
      return html`
        <paper-checkbox class="padd-right" ?checked="${this._isChecked(evidence.id, answer.evidences)}"
          ?requires-description="${evidence.requires_description}"
          @change="${(e: CustomEvent) => this._showOtherInput((e.target! as PaperCheckboxElement))}">
            ${evidence.label}
        </paper-checkbox>`;
    });
  }

  _getRatingRadioButtonsTemplate(question: Question) {
    return question.ratings.map((r: Rating, index: number) =>
      html`<paper-radio-button class="${this._getRatingRadioClass(index)}" name="${r.id}">
             ${r.label}
           </paper-radio-button>`);
  }

  _getRatingRadioClass(index: number)  {
    switch (index) {
      case 0:
        return 'move-left red';
      case 1:
        return 'orange';
      case 2:
        return 'green';
      default:
        return '';
    }
  }

  _isChecked(evidenceId: string, selectedEvidenceIds: string[]) {
    if (!selectedEvidenceIds || !selectedEvidenceIds.length) {
      return false;
    }
    return selectedEvidenceIds.includes(evidenceId);
  }

  _showOtherInput(target: PaperCheckboxElement) {
    const requiresDesc = target.hasAttribute('requires-description');
    if (requiresDesc) {
      this.showOtherInput = !this.showOtherInput;
    }
  }
}

window.customElements.define('questionnaire-answer', QuestionnaireAnswer);