import {
  LitElement, html, customElement
} from 'lit-element';
import {elevationStyles} from '../../../../styles/lit-styles/elevation-styles';

@customElement('answer-instructions')
export class AnswerInstructions extends LitElement {

  static get styles() {
    return [elevationStyles];
  }

  render() {
    // language=HTML
    return html`
      <style>
        paper-tooltip {
          --paper-tooltip-background: #FFFFFF;
          --paper-tooltip: {
            padding: 0;
          }
          width: 80%;
        }
        
        paper-tooltip span {
          font-size: 16px;
          color: var(--primary-text-color);
          line-height: 20px;
        }
        
        #rating-icon{
          color: var(--primary-color);
        }
        
        .rating-info-content {
          padding: 24px;
        }
        
        .rating-info {
          display: flex;
          flex-direction: column;
          padding: 6px;
          margin: 10px 0px;
          width: 100%;
          box-sizing: border-box;
        }
        
        .rating-info.red-border {
          border: solid 1px var(--primary-shade-of-red);
        }
        .rating-info.orange-border {
          border: solid 1px var(--primary-shade-of-orange);
        }
        .rating-info.green-border {
          border: solid 1px var(--primary-shade-of-green);
        }
        
        .rating-info span {
          font-size: 14px;
        }
        
        .rating-info span.primary {
          font-weight: bold;
        }
      </style>
      
      <paper-icon-button id="rating-icon" icon="info"></paper-icon-button>
      <paper-tooltip for="rating-icon" animation-entry="noanimation" position="right">
          ${this.getRatingInfoHtml()}
      </paper-tooltip>
    `;
  }

  getRatingInfoHtml() {
    return html`
      <div class='rating-info-content elevation' elevation="1">
        <div class="rating-info red-border">
            <span class="primary">1 - Absent: The organization is not working towards this standard</span>
            <span>Give this score if the organization meets one of the criteria</span>
        </div>
        <div class="rating-info orange-border">
          <span class="primary">
          2 -Progressing: The organization has made some progress towards applying this standard, but certain
          aspecs need to be improved
          </span>
          <span>Give this score if the organization meets one or two of the three criteria</span>
        </div>
        <div class="rating-info green-border">
          <span class="primary">3 - Adequate: The organization fully meets this standard</span>
          <span>Give this score if the organization meets all of the three criteria</span>
        </div>
      </div>
    `;
  }
}