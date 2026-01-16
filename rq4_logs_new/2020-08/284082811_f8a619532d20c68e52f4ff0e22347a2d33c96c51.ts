import { Attribute } from 'libxmljs';

import { IMemberNames, IImportsTracker } from './DocumentTranslator';
import { IObjectNames } from './ObjectNames';

const expressionRegex: RegExp = /^\{(.+)\}$/;
const bindingKeyRegex: RegExp = /^this(\.[a-zA-Z_][a-zA-Z0-9_]*)+$/;
const objectParsers: Map<string, RegExp> = new Map([
  ['Vector3', /(\-?((([1-9]\d*\.?|0?\.)\d+)|\d)),(\-?((([1-9]\d*\.?|0?\.)\d+)|\d)),(\-?((([1-9]\d*\.?|0?\.)\d+)|\d))/],
  ['Color3', /((0?\.\d+|0|1),(0?\.\d+|0|1),(0?\.\d+|0|1))|'#[0-9a-fA-F]{6}'/],
  ['Color4', /((0?\.\d+|0|1),(0?\.\d+|0|1),(0?\.\d+|0|1),(0?\.\d+|0|1))|'#[0-9a-fA-F]{8}'/]
]);
const modelLoaderEvents: Set<string> = new Set([
  'loaded', 'loading', 'loadFailed', 'progress'
]);

export interface IAttributeInfo {
  name: string;
  value: string;
  propertySetter: string;
  addBinding?: string;
}

export default class AttributeTranslator {
  private readonly _importsTracker: IImportsTracker; // TODO figure out what to do with this
  private readonly _memberNames: IMemberNames;

  constructor(importsTracker: IImportsTracker, memberNames: IMemberNames) {
    this._importsTracker = importsTracker;
    this._memberNames = memberNames;
  }

  public translate(attribute: Attribute, objectType: string, objectNames: IObjectNames): IAttributeInfo {
    const name: string = attribute.name();
    const value: string = attribute.value().trim();

    const expression: string | undefined = this._tryExtractExpression(value);
    if (expression) {
      return this._handleExpression(name, expression, objectType, objectNames);
    } else if (objectType !== 'Custom') {
      const parsedObject: string | undefined = this._tryParseObject(value);
      if (parsedObject) {
        return {
          name,
          value: parsedObject,
          propertySetter: `this.${objectNames.privateName}.${name} = ${parsedObject};`
        };
      }
    }

    return {
      name,
      value: value,
      propertySetter: `this.${objectNames.privateName}.${name} = ${value};`
    };
  }

  public extractExpression(attribute: Attribute): string {
    const value: string = attribute.value().trim();
    return this._tryExtractExpression(value) ?? value;
  }

  public extractExpressionFromTypeAndValue(type: string, value: string): string {
    type = type.trim();
    value = value.trim();
    const expression: string | undefined = this._tryExtractExpression(value);
    if (expression) {
      return expression;
    }

    for (const [objectType, regex] of objectParsers) {
      if (objectType === type && regex.test(value)) {
        return this._parseObject(type, value);
      }
    }

    return value;
  }

  private _tryExtractExpression(value: string): string | undefined {
    const expressionMatch: RegExpMatchArray | null = value.match(expressionRegex);
    if (expressionMatch) {
      return this._cleanExpression(expressionMatch[1]);
    }
  }

  private _handleExpression(
    attributeName: string,
    expression: string,
    objectType: string,
    objectNames: IObjectNames
  ): IAttributeInfo {
    if (objectType === 'ModelLoader' && modelLoaderEvents.has(attributeName)) {
      return {
        name: attributeName,
        value: expression,
        propertySetter: `this.${objectNames.privateName}.${attributeName}.subscribe(${expression});`
      };
    } else if (objectType !== 'Custom' && attributeName === 'enabled') {
      const propertySetterTemplate: string = `this.${objectNames.privateName}.setEnabled(@value);`;
      return {
        name: attributeName,
        value: expression,
        propertySetter: propertySetterTemplate.replace('@value', expression),
        addBinding: this._getAddBinding(expression, propertySetterTemplate)
      };
    } else {
      const propertySetterTemplate: string = `this.${objectNames.privateName}.${attributeName} = @value;`;
      return {
        name: attributeName,
        value: expression,
        propertySetter: propertySetterTemplate.replace('@value', expression),
        addBinding: this._getAddBinding(expression, propertySetterTemplate)
      };
    }
  }

  private _cleanExpression(expression: string): string {
    return expression.trim().replace('this.', `this.${this._memberNames.host}.`);
  }

  private _getAddBinding(expression: string, propertySetterTemplate: string): string | undefined {
    return bindingKeyRegex.test(expression)
      ? `this.${this._memberNames.addBinding}(${expression}, (value) => { ${propertySetterTemplate.replace('@value', 'value')} })`
      : undefined;
  }

  private _tryParseObject(value: string): string | undefined {
    for (const [type, regex] of objectParsers) {
      if (regex.test(value)) {
        return this._parseObject(type, value);
      }
    }
  }

  private _parseObject(type: string, value: string): string {
    if (type.startsWith('Color') && value.startsWith('\'#')) {
      return `${type}.FromHexString(${value})`;
    } else {
      return `new ${type}(${value})`;
    }
  }
}