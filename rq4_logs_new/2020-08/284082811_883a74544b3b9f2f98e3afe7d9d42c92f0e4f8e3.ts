import { Element, Attribute } from 'libxmljs';

import ClassBuilder from '../builders/ClassBuilder';
import { IMemberNames, IImportsTracker } from './DocumentTranslator';
import AttributeTranslator, { IAttributeInfo } from './AttributeTranslator';
import { IObjectNames, getObjectNames, declareObject } from './ObjectNames';
import MethodBuilder from '../builders/MethodBuilder';

const guiObservables: Set<string> = new Set([
  'onPointerClick', 'onPointerDown', 'onPointerEnter', 'onPointerMove', 'onPointerOut', 'onPointerUp',
  'onImageLoaded', 'onIsCheckedChanged',
  'onBeforeKeyAdd', 'onBlur', 'onFocus', 'onKeyboardEventProcessed', 'onTextChanged', 'onTextCopy', 'onTextCut',
  'onTextHighlight', 'onTextPaste'
]);
const stringAttributes: Set<string> = new Set([
  'clipboardData', 'hoverCursor', 'fontFamily', 'fontStyle', 'fontWeight', 'text', 'source', 'promptMessage',
  'currentKey', 'highlightedText', 'margin', 'placeholderText', 'group', 'fontSize', 'height', 'left',
  'paddingBottom', 'paddingLeft', 'paddingRight', 'paddingTop', 'width', 'lineSpacing', 'maxWidth', 'x1', 'x2',
  'y1', 'y2', 'barOffset', 'thumbWidth', 'background', 'color', 'disabledColor', 'disabledColorItem', 'shadowColor',
  'outlineColor', 'focusedBackground', 'focusedColor', 'placeholderColor', 'textHighlightColor', 'borderColor'
]);

export default class GuiTranslator {
  private readonly _class: ClassBuilder;
  private readonly _importsTracker: IImportsTracker;
  private readonly _memberNames: IMemberNames;
  private readonly _meshInitMethods: Map<string, MethodBuilder>;
  private readonly _attributeTranslator: AttributeTranslator;

  constructor(
    classBuilder: ClassBuilder,
    importsTracker: IImportsTracker,
    memberNames: IMemberNames,
    meshInitMethods: Map<string, MethodBuilder>
  ) {
    this._class = classBuilder;
    this._importsTracker = importsTracker;
    this._memberNames = memberNames;
    this._meshInitMethods = meshInitMethods;
    this._attributeTranslator = new AttributeTranslator(importsTracker, memberNames);
  }

  public translate(guisSection: Element): void {
    this._importsTracker.babylon.add('AdvancedDynamicTexture');
    this._importsTracker.babylon.add('Control');
    guisSection.childNodes().forEach(guiElement => {
      if (guiElement.name() === 'FullScreen') {
        this._translateFullScreenGui(guiElement);
      } else if (guiElement.name() === 'TextureGui') {
        this._translateTextureGui(guiElement);
      }
    });
  }

  private _translateFullScreenGui(guiElement: Element): void {
    const objectNames: IObjectNames = getObjectNames('AdvancedDynamicTexture', guiElement.attr('name')?.value());
    declareObject('AdvancedDynamicTexture', objectNames, false, this._class);
    const initMethod: MethodBuilder = this._class.getMethod(this._memberNames.init)!;
    initMethod.addToBody(
      `this.${objectNames.privateName} = AdvancedDynamicTexture.CreateFullscreenUI(${objectNames.publicName}, undefined, this.${this._memberNames.scene});`
    );

    guiElement.attrs()
      .filter(a => a.name() !== 'name')
      .map(a => this._attributeTranslator.translate(a, objectNames.privateName, { doNotParsePrimitives: true }))
      .forEach(info => {
        initMethod.addToBody(info.propertySetter);
        if (info.addBinding) {
          initMethod.addToBody(info.addBinding);
        }
      });

    guiElement.childNodes().forEach(child => {
      this._translateGuiControlsRecursive(child, objectNames.privateName, initMethod);
    });
  }

  private _translateTextureGui(guiElement: Element): void {
    const objectNames: IObjectNames = getObjectNames('AdvancedDynamicTexture', guiElement.attr('name')?.value());
    const ctorArgNames: string[] = ['mesh', 'width', 'height'];
    const ctorArgs: string[] = ctorArgNames.map(arg => {
      const attribute: Attribute | null = guiElement.attr(arg);
      return attribute ? this._attributeTranslator.extractExpression(attribute) : 'undefined';
    });

    const meshIdentifier: string = ctorArgs[0];
    if (!this._meshInitMethods.has(meshIdentifier)) {
      throw new Error(`Referenced mesh not found: ${meshIdentifier}`);
    }

    const initMethod: MethodBuilder = this._meshInitMethods.get(meshIdentifier)!;
    declareObject('AdvancedDynamicTexture', objectNames, initMethod.name !== this._memberNames.init, this._class);

    const meshName: string = getObjectNames('Mesh', meshIdentifier).privateName;
    initMethod.addToBody(
      `this.${objectNames.privateName} = AdvancedDynamicTexture.CreateForMesh(this.${meshName}, ${ctorArgs[1]}, ${ctorArgs[2]});`
    );

    const ctorArgSet: Set<string> = new Set(ctorArgNames);
    guiElement.attrs()
      .filter(a => a.name() !== 'name' && !ctorArgSet.has(a.name()))
      .map(a => {
        return this._attributeTranslator.translate(a, objectNames.privateName, {
          doNotParsePrimitives: true,
          propertyPathOverride: a.name() === 'alpha' ? `${meshName}.material!` : undefined
        });
      })
      .forEach(info => {
        initMethod.addToBody(info.propertySetter);
        if (info.addBinding) {
          initMethod.addToBody(info.addBinding);
        }
      });

    guiElement.childNodes().forEach(child => {
      this._translateGuiControlsRecursive(child, objectNames.privateName, initMethod);
    });
  }

  private _translateGuiControlsRecursive(
    controlElement: Element,
    parentName: string,
    initMethod: MethodBuilder
  ): void {
    const controlType: string = controlElement.name();
    this._importsTracker.babylon.add(controlType);
    const objectNames: IObjectNames = getObjectNames(controlType, controlElement.attr('name')?.value());
    declareObject(controlType, objectNames, initMethod.name !== this._memberNames.init, this._class);

    initMethod.addToBody(
      `this.${objectNames.privateName} = new ${controlType}(${objectNames.publicName});`,
      `this.${parentName}.addControl(this.${objectNames.privateName});`
    );

    controlElement.attrs()
      .filter(a => a.name() !== 'name')
      .map(a => this._attributeTranslator.translate(a, objectNames.privateName, {
        doNotParsePrimitives: true,
        updateMethod: a.name() === 'linkWithMesh' ? 'linkWithMesh' : undefined,
        isObservable: guiObservables.has(a.name()),
        useQuotesIfNeeded: stringAttributes.has(a.name())
      }))
      .forEach(info => {
        initMethod.addToBody(info.propertySetter);
        if (info.addBinding) {
          initMethod.addToBody(info.addBinding);
        }
      });

    controlElement.childNodes().forEach(child => {
      this._translateGuiControlsRecursive(child, objectNames.privateName, initMethod);
    });
  }
}