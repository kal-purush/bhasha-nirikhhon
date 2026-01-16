import { RestCall } from 'src/email/dto/rest-call';

interface Email {
  sendEmail: any;
}

class DefaultEmail implements Email {
  sendEmail = () => {};
}

class SignupEmail implements Email {
  sendEmail = (mailchimp: any, data: RestCall) => {
    const {
      emailSubject: subject,
      emailsToSendTo,
      objectId: userId,
      userFullName,
    } = data;

    const to = emailsToSendTo.map((email: any) => ({
      email,
      type: 'to',
    }));

    const message = {
      from_email: 'tech@puente-dr.org',
      to,
      subject,
      global_merge_vars: [
        {
          name: 'objectId',
          content: userId,
        },
        {
          name: 'full_name',
          content: userFullName,
        },
      ],
    };

    return mailchimp.messages.sendTemplate({
      template_name: 'acela-signup-verification',
      template_content: [{}],
      message,
    });
  };
}

class PasswordResetEmail implements Email {
  sendEmail = (mailchimp: any, data: RestCall) => {
    const {
      emailSubject: subject,
      emailsToSendTo,
      objectId: userId,
      userFullName,
    } = data;

    const to = emailsToSendTo.map((email: any) => ({
      email,
      type: 'to',
    }));

    const message = {
      from_email: 'tech@puente-dr.org',
      to,
      subject,
      global_merge_vars: [
        {
          name: 'objectId',
          content: userId,
        },
        {
          name: 'full_name',
          content: userFullName,
        },
      ],
    };

    return mailchimp.messages.sendTemplate({
      template_name: 'acela-password-reset',
      template_content: [{}],
      message,
    });
  };
}

const emailTypeMap = {
  default: DefaultEmail,
  signup: SignupEmail,
  passwordReset: PasswordResetEmail,
};

type EmailMap = typeof emailTypeMap;
type Keys = keyof EmailMap;
type Tuples<T> = T extends Keys ? [T, InstanceType<EmailMap[T]>] : never;
type SingleKeys<K> = [K] extends (K extends Keys ? [K] : never) ? K : never;
type ClassType<A extends Keys> = Extract<Tuples<Keys>, [A, any]>[1];

export class EmailFactory {
  static getEmailType<K extends Keys>(k: SingleKeys<K>): ClassType<K> {
    return new emailTypeMap[k]();
  }
}