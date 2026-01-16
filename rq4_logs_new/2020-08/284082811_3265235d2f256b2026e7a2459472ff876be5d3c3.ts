import { Element } from 'libxmljs';

import ClassBuilder from '../builders/ClassBuilder';
import MethodBuilder from '../builders/MethodBuilder';
import { IMemberNames, IImportsTracker, ISharedObjects } from './DocumentTranslator';
import { IObjectNames, getObjectNames, declareObject } from './ObjectNames';
import AttributeTranslator from './AttributeTranslator';
import TextureTranslator from './TextureTranslator';

const textureSlots: Map<string, Set<string>> = new Map([
  ['StandardMaterial', new Set([
    'ambientTexture',
    'bumpTexture',
    'cameraColorGradingTexture',
    'diffuseTexture',
    'emissiveTexture',
    'lightmapTexture',
    'opacityTexture',
    'reflectionTexture',
    'refractionTexture',
    'specularTexture'
  ])]
]);

export default class MaterialTranslator {
  private readonly _class: ClassBuilder;
  private readonly _importsTracker: IImportsTracker;
  private readonly _sharedObjects: ISharedObjects;
  private readonly _memberNames: IMemberNames;
  private readonly _attributeTranslator: AttributeTranslator;

  constructor(
    classBuilder: ClassBuilder,
    importsTracker: IImportsTracker,
    sharedObjects: ISharedObjects,
    memberNames: IMemberNames
  ) {
    this._class = classBuilder;
    this._importsTracker = importsTracker;
    this._sharedObjects = sharedObjects;
    this._memberNames = memberNames;
    this._attributeTranslator = new AttributeTranslator(importsTracker, memberNames);
  }

  public translateSharedMaterial(materialElement: Element): void {
    const materialType: string = materialElement.name();
    const name: string = materialElement.attr('name')!.value();
    const objectNames: IObjectNames = getObjectNames(materialType, name);
    const ensureMethodName: string = `_ensure_${name}`;
    const hasEnsureMethod: boolean = this._class.hasMethod(ensureMethodName);
    let initMethod: MethodBuilder;

    if (hasEnsureMethod) {
      initMethod = this._class.getMethod(ensureMethodName)!;
      declareObject(materialType, objectNames, true, this._class);
      initMethod.addToBody(
        `if (this.${objectNames.privateName}) return this.${objectNames.privateName};`
      );
    } else {
      initMethod = this._class.getMethod(this._memberNames.init)!;
      declareObject(materialType, objectNames, false, this._class);
    }

    this._translateMaterial(materialElement, objectNames, initMethod);

    if (hasEnsureMethod) {
      initMethod.addToBody(`return this.${objectNames.privateName};`);
    }
  }

  public translateNodeMaterial(materialElement: Element, nodeName: string, deferredGroup?: string): void {
    const materialType: string = materialElement.name();
    const name: string | undefined = materialElement.attr('name')?.value();
    const objectNames: IObjectNames = getObjectNames(materialType, name);
    declareObject(materialType, objectNames, !!deferredGroup, this._class);

    const initMethod: MethodBuilder = this._class.getMethod(deferredGroup ?? this._memberNames.init)!;
    this._translateMaterial(materialElement, objectNames, initMethod, nodeName, deferredGroup);
  }

  private _translateMaterial(
    materialElement: Element,
    objectNames: IObjectNames,
    initMethod: MethodBuilder,
    nodeName?: string,
    deferredGroup?: string
  ): void {
    const materialType: string = materialElement.name();
    this._importsTracker.babylon.add(materialType);
    initMethod.addToBody(
      `this.${objectNames.privateName} = new ${materialType}('${objectNames.publicName}', this.${this._memberNames.scene});`
    );
    if (nodeName) {
      initMethod.addToBody(`this.${nodeName}.material = this.${objectNames.privateName};`);
    }

    const materialTextureSlots: Set<string> = textureSlots.get(materialType)!;
    materialElement.attrs()
      .filter(a => a.name() !== 'name')
      .map(a => this._attributeTranslator.translate(a, materialType, objectNames))
      .forEach(info => {
        if (materialTextureSlots.has(info.name)) {
          this._handleTextureAttribute(objectNames.privateName, info.name, info.value, initMethod);
        } else {
          initMethod.addToBody(info.propertySetter);
          if (info.addBinding) {
            initMethod.addToBody(info.addBinding);
          }
        }
      });

    materialElement.childNodes().forEach(textureSlotElement => {
      this._translateTexture(textureSlotElement, objectNames.privateName, deferredGroup);
    });
  }

  private  _handleTextureAttribute(
    materialName: string,
    textureSlot: string,
    textureName: string,
    materialInitMethod: MethodBuilder
  ): void {
    if (!this._sharedObjects.textures.has(textureName)) {
      throw new Error(`Shared texture not found: ${textureName}`);
    }
    const ensureMethodName: string = `_ensure_${textureName}`;
    if (!this._class.hasMethod(ensureMethodName)) {
      const ensureMethod: MethodBuilder = new MethodBuilder(
        ensureMethodName,
        'Texture',
        'private'
      );
      this._class.addMethod(ensureMethod);
    }
    materialInitMethod.addToBody(
      `this.${materialName}.${textureSlot} = this.${ensureMethodName}();`
    );
  }

  private _translateTexture(textureSlotElement: Element, parentName: string, deferredGroup?: string): void {
    let textureSlot: string = textureSlotElement.name();
    textureSlot = textureSlot[0].toLowerCase() + textureSlot.slice(1);
    if ((textureSlotElement.parent() as Element).attr(textureSlot)) {
      throw new Error(`Conflicting texture assignments on material ${parentName.slice(2)}.${textureSlot}.`);
    }

    const textureTranslator: TextureTranslator = new TextureTranslator(
      this._class,
      this._importsTracker,
      this._memberNames
    );
    textureTranslator.translateMaterialTexture(
      textureSlotElement.child(0)!,
      { name: parentName, textureSlot },
      deferredGroup
    );
  }
}