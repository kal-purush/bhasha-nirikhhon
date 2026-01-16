import { Meteor } from "meteor/meteor";
import { TransactionsCollection } from "./TransactionsCollection";
import SimpleSchema from "simpl-schema";

Meteor.methods({
  "transactions.insert"(args) {
    const schema = new SimpleSchema({
      isTransferring: {
        type: Boolean,
      },
      sourceWalletId: {
        type: String,
      },
      destinationWalletId: {
        type: String,
        optional: !args.isTransferring,
      },
      amount: {
        type: Number,
        min: 1,
      },
    });
    const cleanData = schema.clean(args);
    schema.validate(cleanData);
    const { isTransferring, sourceWalletId, destinationWalletId, amount } =
      args;

    return TransactionsCollection.insertAsync({
      type: isTransferring ? "TRANSFER" : "ADD",
      sourceWalletId,
      destinationWalletId: isTransferring ? destinationWalletId : null,
      amount,
      createdAt: new Date(),
    });
  },
});