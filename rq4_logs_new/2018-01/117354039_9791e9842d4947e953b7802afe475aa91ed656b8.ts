import Context from "../context";
import { PrototypeDefinition } from "../runtime";
import { PlorthValue } from "plorth-types";

const ArrayPrototype: PrototypeDefinition = {
  length(context: Context) {
    context.pushNumber(context.peekArray().length);
  },

  push(context: Context) {
    const array = context.popArray();
    const value = context.pop();

    context.pushArray(...array, value);
  },

  pop(context: Context) {
    // TODO
  },

  "includes?"(context: Context) {
    // TODO
  },

  "index-of"(context: Context) {
    // TODO
  },

  find(context: Context) {
    const array = context.popArray();
    const quote = context.popQuote();

    context.pushArray(...array);
    array.forEach(value => {
      context.push(value);
      context.call(quote);
      if (context.popBoolean()) {
        context.push(value);
        return;
      }
    });
    context.push(null);
  },

  "find-index"(context: Context) {
    // TODO
  },

  "every?"(context: Context) {
    const array = context.popArray();
    const quote = context.popQuote();

    context.pushArray(...array);
    array.forEach(value => {
      context.push(value);
      context.call(quote);
      if (!context.popBoolean()) {
        context.pushBoolean(false);
        return;
      }
    });
    context.pushBoolean(true);
  },

  "some?"(context: Context) {
    const array = context.popArray();
    const quote = context.popQuote();

    context.pushArray(...array);
    array.forEach(value => {
      context.push(value);
      context.call(quote);
      if (context.popBoolean()) {
        context.pushBoolean(true);
        return;
      }
    });
    context.pushBoolean(false);
  },

  reverse(context: Context) {
    // TODO
  },

  uniq(context: Context) {
    // TODO
  },

  extract(context: Context) {
    const array = context.popArray();

    for (let i = array.length; i > 0; --i) {
      context.push(array[i - 1]);
    }
  },

  join(context: Context) {
    // TODO
  },

  ">quote"(context: Context) {
    // TODO
  },

  "for-each"(context: Context) {
    const array = context.popArray();
    const quote = context.popQuote();

    array.forEach(value => {
      context.push(value);
      context.call(quote);
    });
  },

  "2for-each"(context: Context) {
    // TODO
  },

  map(context: Context) {
    const array = context.popArray();
    const quote = context.popQuote();
    const result: Array<PlorthValue | null> = [];

    array.forEach(value => {
      context.push(value);
      context.call(quote);
      result.push(context.pop());
    });
    context.pushArray(...result);
  },

  "2map"(context: Context) {
    // TODO
  },

  filter(context: Context) {
    const array = context.popArray();
    const quote = context.popQuote();
    const result: Array<PlorthValue | null> = [];

    array.forEach(value => {
      context.push(value);
      context.call(quote);
      if (context.popBoolean()) {
        result.push(value);
      }
    });
    context.pushArray(...result);
  },

  reduce(context: Context) {
    // TODO
  },

  "+"(context: Context) {
    const a = context.popArray();
    const b = context.popArray();

    context.pushArray(...b, ...a);
  },

  "*"(context: Context) {
    // TODO
  },

  "&"(context: Context) {
    // TODO
  },

  "|"(context: Context) {
    // TODO
  },

  "@"(context: Context) {
    // TODO
  },

  "!"(context: Context) {
    // TODO
  }
};

export default ArrayPrototype;