import mongoose from 'mongoose';
import { OrderStatus } from '@gravitaz/common';
import { TicketDoc } from './ticket';

// an interface that describes the properties that are required to create a new Order
interface OrderAttrs {
  ticket: TicketDoc;
  status: OrderStatus;
  expiresAt: Date;
  userId: string;
}

// An interface that describes the properties
// that a Order model has

interface OrderModel extends mongoose.Model<OrderDoc> {
  build(attrs: OrderAttrs): OrderDoc;
}

// An interface that describes the properties
// that a OrderDocument has
interface OrderDoc extends mongoose.Document {
  ticket: TicketDoc;
  status: OrderStatus;
  expiresAt: Date;
  userId: string;
}

const OrderSchema = new mongoose.Schema(
  {
    ticket: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'Ticket',
      required: true,
    },
    status: {
      type: String,
      required: true,
      enum: Object.values(OrderStatus),
      default: OrderStatus.Created,
    },
    expiresAt: {
      type: mongoose.Schema.Types.Date,
    },
    userId: {
      type: String,
      required: true,
    },
  },
  {
    toJSON: {
      transform(doc, ret) {
        ret.id = ret._id;
        delete ret._id;
      },
    },
  }
);

OrderSchema.statics.build = (attrs: OrderAttrs) => {
  return new Order(attrs);
};

const Order = mongoose.model<OrderDoc, OrderModel>('Order', OrderSchema);

export { Order, OrderDoc, OrderAttrs };