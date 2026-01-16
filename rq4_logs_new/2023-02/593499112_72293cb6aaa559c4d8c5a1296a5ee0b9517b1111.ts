export class DeleteMessageRequestModel {
  private queueUrl: string
  private receiptHandle: string


  constructor(queueUrl: string, receiptHandle: string) {
    this.queueUrl = queueUrl;
    this.receiptHandle = receiptHandle;
  }
}