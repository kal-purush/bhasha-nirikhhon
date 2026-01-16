import { SummaryStepPage } from './summary-step.page';
import { SignInStepPage } from './sign-in-step.page';
import { AddressStepPage } from './address-step.page';
import { ShippingStepPage } from './shipping-step.page';
import { PaymentStepPage } from './payment-step.page';

export class OrderSummaryPage {
  private summaryStepPage: SummaryStepPage;
  private signInStepPage: SignInStepPage;
  private addressStepPage: AddressStepPage;
  private shippingStepPage: ShippingStepPage;
  private paymentStepPage: PaymentStepPage;

  constructor() {
    this.summaryStepPage = new SummaryStepPage();
    this.signInStepPage = new SignInStepPage();
    this.addressStepPage = new AddressStepPage();
    this.shippingStepPage = new ShippingStepPage();
    this.paymentStepPage = new PaymentStepPage();
  }

  public get summaryStep() {
    return this.summaryStepPage;
  }

  public get signInStep() {
    return this.signInStepPage;
  }

  public get addressStep() {
    return this.addressStepPage;
  }

  public get shippingStep() {
    return this.shippingStepPage;
  }

  public get paymentStep() {
    return this.paymentStepPage;
  }
}