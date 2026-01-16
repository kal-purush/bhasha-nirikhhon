import React, { useContext } from 'react';
import IntextStep from 'components/shared/In-textStep/In-textStep';
import InsetText from 'components/shared/InsetText/InsetText';
import { FormDataContext } from '../../../globalState/FormDataContext';

function Success() {
  // eslint-disable-next-line no-unused-vars
  const [formDataState, formDataDispatch] = useContext(FormDataContext);
  const { Phone, SMSAlert, EmailAlert, SMSTerms } = formDataState.formData;

  const alignCenter = {
    textAlign: 'center',
  };

  let message;
  let steps;

  if (Phone && (SMSAlert === 'yes' || SMSTerms) && EmailAlert === 'yes') {
    /* Text messages AND Email */
    message = 'You have successfully signed up to text message and email alerts';
    steps = [
      'We’ll send you an email asking to confirm your subscription.',
      'When you confirm your subscription, we’ll send a PIN code via text message. The PIN code can take up to 5 minutes to arrive.',
      'Visit the link in the confirmation email to access your disruption alert dashboard. Enter the PIN code sent to you via text message.',
      'Once you have confirmed your mobile phone number, you’ll receive disruption alerts to your mobile phone.',
    ];
  } else if (Phone && (SMSAlert === 'yes' || SMSTerms)) {
    /* Text messages */
    message = 'You have successfully signed up to text message alerts';
    steps = [
      'We’ll send you an email asking to confirm your subscription for text message alerts.',
      'When you confirm your subscription, we’ll send a PIN code via text message. The PIN code can take up to 5 minutes to arrive.',
      'Visit the link in the confirmation email to access your disruption alert dashboard. Enter the PIN code sent to you via text message.',
      'Once you have confirmed your mobile phone number, you’ll receive disruption alerts to your mobile phone.',
    ];
  } else if (!Phone) {
    /* Emails only */
    message = 'You have successfully signed up to email alerts';
    steps = [
      'We’ll send you an email asking to confirm your subscription.',
      'Once you have confirmed your subscription, you’ll receive disruption alerts to your email address.',
    ];
  }

  return (
    <div className="wmnds-container-alerts-sign-up">
      <div className="wmnds-grid wmnds-col-md-2-3 wmnds-col-lg-1-2">
        <div className="wmnds-col-1">
          <h1>Sign up to service disruption alerts</h1>
        </div>
        <div className="wmnds-col-1 wmnds-m-b-xl">
          <div style={alignCenter} className="wmnds-msg-summary wmnds-msg-summary--success-fill ">
            <div className="wmnds-msg-summary__header">
              <h3 className="wmnds-msg-summary__title">{message}</h3>
            </div>
          </div>
        </div>

        <div className="wmnds-col-1">
          <h3>What happens next</h3>
          <IntextStep steps={steps} />
          <InsetText
            classes="wmnds-m-b-lg"
            content="The disruption alerts dashboard is where you can manage your communication preferences. You can access the dashboard at any time by visiting the link in your confirmation email."
          />

          <p>
            To unsubscribe from all disruption alerts, please select the ‘Unsubscribe from alerts’
            option in the disruption alerts dashboard.
          </p>

          <p>
            <a
              href="https://surveys.hotjar.com/s?siteId=264586&surveyId=158470"
              title="Service feedback survey"
              target="_blank"
              className="wmds-link"
              rel="noopener noreferrer"
            >
              What did you think of this service?
            </a>{' '}
            (takes 30 seconds)
          </p>
        </div>
      </div>
    </div>
  );
}

export default Success;