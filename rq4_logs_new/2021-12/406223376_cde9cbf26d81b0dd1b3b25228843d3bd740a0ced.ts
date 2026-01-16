//FRAPPE-81
import { EventSubscriberInterface, EventSubscribersMeta } from "@tshio/event-dispatcher";
import { PaymentProvider } from '@frappe/payment/domain'
import { Product, ProductCreated, ProductUpdated } from "@frappe/product/domain";
import { wrapError } from "@frappe/common/utils";

interface UpdateProductOnStripeDeps {
  readonly paymentProvider: PaymentProvider;
}

export class UpdateProductOnStripe implements EventSubscriberInterface {
  private readonly paymentProvider: PaymentProvider;

  constructor({ paymentProvider }: UpdateProductOnStripeDeps) {
    this.paymentProvider = paymentProvider;
  }

  getSubscribedEvents(): EventSubscribersMeta[] {
    return [
      { name: ProductUpdated.name, method: 'execute' }
    ];
  }

  async execute(event: ProductCreated) {
    const product = Product.fromPrimitives(event.payload)

    const [archiveError] = await wrapError(this.paymentProvider.archivePrice(product.id))

    if(product.isInSale.value){
      const [priceError] = await wrapError(this.paymentProvider.createPrice(product.id.value, product.priceInSale.value));
    } else {
      const [priceError] = await wrapError(this.paymentProvider.createPrice(product.id.value, product.price.value));
    }

  }
}