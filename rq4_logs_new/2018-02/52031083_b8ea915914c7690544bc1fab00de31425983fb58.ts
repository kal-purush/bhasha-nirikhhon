import { assert } from "chai";
import { JsonReader } from "../../lib/readers/json";
import { JsonValueReader } from "../../lib/readers/json-value";
import { AnyType } from "../../lib/types/any";
import { DocumenIoType, DocumentType } from "../../lib/types/document";

describe("AnyType", function () {
  describe("with JsonReader", function () {
    it("should read the expected top-level values", function () {
      const reader: JsonReader = new JsonReader();
      const $Any: AnyType = new AnyType();
      assert.deepEqual($Any.read(reader, "0"), "0");
      assert.deepEqual($Any.read(reader, "{\"foo\": \"bar\""), "{\"foo\": \"bar\"");
    });
    it("should read the expected nested values", function () {
      const reader: JsonReader = new JsonReader();
      const $Any: AnyType = new AnyType();

      interface FooBarQuz {
        foo: any;
      }

      const $FooBarQuz: DocumenIoType<FooBarQuz> = new DocumentType({
        properties: {foo: {type: $Any}},
      });

      assert.deepEqual($FooBarQuz.read(reader, "{\"foo\": {\"bar\": \"quz\"}}"), {foo: {bar: "quz"}});
    });
  });

  describe("with JsonValueReader", function () {
    it("should read the expected values", function () {
      const reader: JsonValueReader = new JsonValueReader();
      const $Any: AnyType = new AnyType();
      assert.deepEqual($Any.read(reader, 0), 0);
      assert.deepEqual($Any.read(reader, {foo: "bar"}), {foo: "bar"});
    });
  });
});