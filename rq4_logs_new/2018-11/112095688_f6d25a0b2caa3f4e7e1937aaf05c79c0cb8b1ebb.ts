// https://docs.aws.amazon.com/ses/latest/DeveloperGuide/event-publishing-retrieving-sns-contents.html

export interface SesEventToSns {
  eventType: 'Delivery' | 'Send' | 'Reject' | 'Open' | 'Click' | 'Bounce' | 'Complaint' | 'Rendering Failure';
  mail: Mail;
  bounce?: Bounce;
  complaint?: Complaint;
  delivery?: Delivery;
  send?: Send;
  reject?: Reject;
  open?: Open;
  click?: Click;
  failure?: Failure;
}

export interface Mail {
  timestamp: string;
  source: string;
  sourceArn: string;
  sendingAccountId: string;
  messageId: string;
  destination?: Array<string> | null;
  headersTruncated: boolean;
  headers?: Array<HeadersEntity> | null;
  commonHeaders: CommonHeaders;
  tags: any;
}

export interface HeadersEntity {
  name: string;
  value: string;
}

export interface CommonHeaders {
  from?: Array<string> | null;
  to?: Array<string> | null;
  messageId: string;
  subject: string;
}

export interface Bounce {
  bounceType: 'Undetermined' | 'Permanent' | 'Transient';
  bounceSubType: 'Undetermined' | 'General' | 'NoEmail' | 'Suppressed' | 'General' | 'MailboxFull' | 'MessageTooLarge' | 'ContentRejected' | 'AttachmentRejected';
  bouncedRecipients: Array<BouncedRecipient>;
  timestamp: string;
  feedbackId: string;
  reportingMTA?: string;
}

export interface BouncedRecipient {
  emailAddress: string;
}

export interface Complaint {
  complainedRecipients: Array<ComplainedRecipient> | null;
  timestamp: string;
  feedbackId: string;
  userAgent?: string;
  complaintFeedbackType: string;
  arrivalDate: string;
}

export interface ComplainedRecipient {
  emailAddress: string;
}

export interface Delivery {
  timestamp: string;
  processingTimeMillis: number;
  recipients: any;
  smtpResponse: string;
  reportingMTA: string;
}

export interface Send {
}

export interface Reject {
  reason: string;
}

export interface Open {
  ipAddress: string;
  timestamp: string;
  userAgent: string;
}

export interface Click {
  ipAddress: string;
  timestamp: string;
  userAgent: string;
  link: string;
  linkTags: any;
}

export interface Failure {
  templateName: string;
  errorMessage: string;
}

