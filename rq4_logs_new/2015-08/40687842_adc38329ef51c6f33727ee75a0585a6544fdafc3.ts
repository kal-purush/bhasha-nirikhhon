/// <reference path="../typings/bluebird/bluebird.d.ts"/>
/// <reference path="../typings/chalk/chalk.d.ts"/>
/// <reference path="../typings/commander/commander.d.ts"/>
/// <reference path="../typings/deep-diff/deep-diff.d.ts"/>
/// <reference path="../typings/json-stable-stringify/json-stable-stringify.d.ts"/>
/// <reference path="../typings/lodash/lodash.d.ts"/>
/// <reference path="../typings/node/node.d.ts"/>
/// <reference path="../typings/xml2js/xml2js.d.ts"/>

'use strict';

declare function require(name: string): any;
require('source-map-support').install();

import _ = require('lodash');
import assert = require('assert');
import chalk = require('chalk');
import commander = require('commander');
import deepDiff = require('deep-diff');
import fs = require('fs');
import jsonStableStringify = require('json-stable-stringify');
import P = require('bluebird');
import xml2js = require('xml2js');

// Override the default program description.
commander.usage(
  'old.xml new.xml\n\n' +
    '  Outputs the difference between two XML files.\n' +
    '  Exactly two XML files must be specified.'
);

// ### XsdType
// Fake interface to distinguish "any" object produced by xml2js.
interface XsdType {
  $?: XsdAttribute;
}

// ### Dollar
// XSD results will have this element indicating an attribute.
interface XsdAttribute {
  name: string;
}

// ### JsonType
// Fake interface to distinguish "any" object produced by xsdDecode.
interface JsonType {}

// ### XsdDecoder
// Decoder for a certain type of XSD element.
interface XsdDecoder {
  (xsdList: XsdType): JsonType;
}

// ### XsdDecoders
// Table of decoders for each type of XSD element.
interface XsdDecoders {
  [xsdElement: string]: XsdDecoder;
}

// ### xsdDecoders
// Table of decoders for each type of XSD element.
var xsdDecoders: XsdDecoders;

// ### xsdDecodeObject
// Convert the XSD object representation by recursively decoding each property.
function xsdDecodeObject(xsdType: XsdType): JsonType {
  var jsonType: JsonType
    = <JsonType> _.mapValues(xsdType, (child: XsdType, key: string): JsonType => {
      var decoder = xsdDecoders[key];
      return decoder ? decoder(child) : xsdDecode(child);
    });
  return jsonType;
}

// ### xsdDecodeArray
// Recursively decode an XSD array representation.
function xsdDecodeArray(xsdType: XsdType[]): JsonType[] {
  return xsdType.map(xsdDecode);
}

// ### xsdDecode
// Converts the XSD JSON representation into a JSON object that is more suited to diff.
function xsdDecode(xsdType: XsdType): JsonType {
  if (_.isArray(xsdType)) {
    return xsdDecodeArray(<XsdType[]>xsdType);
  } else if (!_.isObject(xsdType)) {
    return <JsonType>xsdType;
  } else {
    return xsdDecodeObject(<XsdType>xsdType);
  }
}

// ### makeNamespace
// Converts the XSD JSON representation of a list into a JSON object representing a namespace.
// The 'name' property of each object in the list is used for the keys of the JSON object.
var makeNamespace: XsdDecoder = function(xsdList: XsdType[]): JsonType {
  if (!xsdList) {
    return undefined;
  }

  var jsonNamespace: any = {};
  _.forEach(xsdList, (xsdElem: XsdType): void => {
    var name: string = '';
    var $: XsdAttribute = xsdElem.$;
    if ($) {
      name = $.name;
    }
    assert(!(name in jsonNamespace));
    jsonNamespace[name] = xsdDecode(xsdElem);
  });
  return jsonNamespace;
};

// List of elements that we will decode in a special way.
xsdDecoders = {
  'xsd:element': makeNamespace,
  'xsd:attribute': makeNamespace,
  'xsd:complexType': makeNamespace,
  'xsd:simpleType': makeNamespace
};

// ### makeSchema
// Converts the XSD JSON representation into a schema JSON object that is more suited to diff.
var makeSchema: XsdDecoder = function(xsdJson: XsdType): JsonType {
  var schema = xsdDecode(xsdJson);
  return schema;
};

// ### diffCompare
// Comparator for sorting JSON produced by deep-diff.
type JSElement = jsonStableStringify.Element;
var diffCompare: jsonStableStringify.Comparator = function(a: JSElement, b: JSElement): number {
  // Compares two keys from a deep-diff object.
  var order = ['kind', 'path', 'index', 'item', 'lhs', 'rhs'];
  var aIndex = order.indexOf(a.key);
  var bIndex = order.indexOf(b.key);
  if (aIndex < 0 && bIndex < 0) {
    return a.key.localeCompare(b.key);
  } else if (aIndex < 0) {
    return +1;
  } else if (bIndex < 0) {
    return -1;
  } else {
    return aIndex - bIndex;
  }
};

// ### MakePretty
// We can make any of the deep-diff result properties pretty using a simple map function.
interface MakePretty {
  (ugly: any): any;
}

// ### makePrettyPath
// Transforms the path from a single raw diff from deep-diff from an array to a string.
var makePrettyPath: MakePretty = function(rawPath: string[]): string {
  assert(_.isArray(rawPath));
  return rawPath.join('/');
};

// ### MakePrettyTable
// Map of each key in the deep-diff result that we want to make pretty.
interface MakePrettyTable {
  [key: string]: MakePretty;
}

// ### makePrettyDiff
// Transform the result of deep-diff into something we're not ashamed of.
function makePrettyDiff(rawDiff: deepDiff.IDiff): deepDiff.IDiff {
  // Transforms a single raw diff from deep-diff to make it more presentable.

  // Transform certain keys.
  var prettyKeys: MakePrettyTable = {
    path: makePrettyPath
  };

  var prettyDiff = _.mapValues(rawDiff, (rawValue: any, key: string): any => {
    var makePretty: MakePretty = prettyKeys[key];
    return makePretty ? makePretty(rawValue) : rawValue;
  });

  return prettyDiff;
}

function makePrettyDiffs(rawDiffs: deepDiff.IDiff[]): deepDiff.IDiff[] {
  // Transforms the raw output of deep-diff to make it more presentable.
  return _.map(rawDiffs, makePrettyDiff);
}

// Choose the appropraite overload of process.stdout.write (socket.write).
interface Write {
  (data: string, cb: (err: Error, result: void) => void): void;
}
var writeP = P.promisify(<Write>process.stdout.write, process.stdout);

function main(args: string[]): P<void> {
  // Create a UTF-8 file reader.
  function readFileUtf8(fileName: string, callback: (err: Error, contents: string) => void) {
    fs.readFile(fileName, 'utf8', callback);
  };
  // Create a P-compatible version of the UTF-8 file reader.
  var readFileP = P.promisify(readFileUtf8);

  // Create a P-compatible version of xml2js.parseString.
  var parseStringP = P.promisify(xml2js.parseString);

  // Read both files.
  return P.all(args.map((xml: string): P<string> => readFileP(xml)))
  // Parse XML into JSON.
    .then((xml: string[]): P<XsdType[]> => P.all(_.map(xml, (xml: string) => parseStringP(xml))))
  // Parse XSD JSON into special schema JSON.
    .then((json: XsdType[]): JsonType[] => _.map(json, (json: XsdType) => makeSchema(json)))
  // Compute the diff of the two JSON schemas.
    .spread((json1: JsonType, json2: JsonType): deepDiff.IDiff[] => deepDiff.diff(json1, json2))
  // Make the diff pretty.
    .then((rawDiffs: deepDiff.IDiff[]): deepDiff.IDiff[] => makePrettyDiffs(rawDiffs))
  // Write to stdout.
    .then((diffJson: deepDiff.IDiff[]): P<void> => {
      // Convert JSON to a string.
      var opts = {
        space: 2,
        cmp: diffCompare
      };
      var diffString: string = jsonStableStringify(diffJson, opts);

      // Write to stdout.
      return writeP(diffString);
    })
    .catch((err: Error): void => {
      var error = chalk.bold.red;
      console.error(error('xsddiff: %s\nTrace:\n%s'), err.toString(), (<any>err).stack);
      process.exit(1);
    });
}

// Parse command line arguments.
commander.parse(process.argv);

if (commander.args.length !== 2) {
  commander.help();
} else {
  main(commander.args);
}