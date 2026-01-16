import ClassBuilder from './ClassBuilder';
import MethodBuilder from './MethodBuilder';
import PropertyBuilder from './PropertyBuilder';

describe('ClassBuilder', () => {
  it('can produce an empty class', () => {
    const builder: ClassBuilder = new ClassBuilder('Empty', 'export default');
    const str: string = builder.toString();
    expect(str).toMatch(/export default class Empty\s*\{\s*\}/);
  });

  it('can produce a non-exported class', () => {
    const builder: ClassBuilder = new ClassBuilder('PrivateClass', 'private');
    const str: string = builder.toString();
    expect(str).toMatch(/class PrivateClass\s*\{\s*\}/);
  });

  it('can produce a class with a base class', () => {
    const builder: ClassBuilder = new ClassBuilder('Derived', 'export');
    builder.baseClass = 'Base';
    const str: string = builder.toString();
    expect(str).toMatch(/export class Derived extends Base\s*\{\s*\}/);
  });

  it('can produce a class implementing multiple interfaces', () => {
    const builder: ClassBuilder = new ClassBuilder('Implementation', 'export');
    builder.addInterface('IFoo');
    builder.addInterface('IBar');
    builder.addInterface('IQux');
    const str: string = builder.toString();
    expect(str).toMatch(/export class Implementation\s* implements IFoo, IBar, IQux \{\s*\}/);
  });

  it('can produce a class with a constructor', () => {
    const builder: ClassBuilder = new ClassBuilder('Foo', 'export');
    builder.setConstructorArgs(
      { name: 'title', type: 'string' },
      { name: 'props', type: 'IFooProps', optional: true }
    );
    builder.addToConstructorBody(
      'this._title = title;',
      'this.props = props ?? {};',
      'this._doStuff();'
    );
    const str: string = builder.toString();
    expect(str).toMatch(
      /export class Foo\s*\{\s*constructor\(title: string, props\?: IFooProps\) \{\s*this\._title = title;\s*this\.props = props \?\? \{\};\s*this\._doStuff\(\);\s*\}\s*\}/
    );
  });

  it('can produce a class with varied member variables', () => {
    const builder: ClassBuilder = new ClassBuilder('Foo', 'export');
    builder.addMemberVariable(
      'x',
      'number',
      'public',
      '0'
    );
    builder.addMemberVariable(
      '_name',
      'string',
      'protected',
      undefined,
      '?'
    );
    builder.addMemberVariable(
      '_props',
      'IProps',
      'private',
      '{}',
      '!'
    );
    const str: string = builder.toString();
    expect(str).toMatch(
      /export class Foo\s*\{\s*public x: number = 0;\s*protected _name\?: string;\s*private _props!: IProps = \{\};\s*\}/
    );
  });

  it('can produce a class with methods', () => {
    const classBuilder: ClassBuilder = new ClassBuilder('Foo', 'export');
    const doStuffBuilder: MethodBuilder = new MethodBuilder('doStuff', 'void', 'public');
    const addValueBuilder: MethodBuilder = new MethodBuilder(
      'addValue',
      'boolean',
      'protected',
      { name: 'x', type: 'number' }
    );
    classBuilder.addMethod(doStuffBuilder);
    classBuilder.addMethod(addValueBuilder);
    const str: string = classBuilder.toString();
    expect(str).toMatch(
      /export class Foo\s*\{\s*public doStuff\(\): void \{\s*\}\s*protected addValue\(x: number\): boolean \{\s*\}\s*\}/
    );
  });

  it('can produce a class with properties', () => {
    const classBuilder: ClassBuilder = new ClassBuilder('Foo', 'export');
    const titleBuilder: PropertyBuilder = new PropertyBuilder('title', 'string', 'public');
    titleBuilder.addToGetterBody('return this._title;');
    titleBuilder.addToSetterBody('this._title = value;');
    classBuilder.addProperty(titleBuilder);
    const str: string = classBuilder.toString();
    expect(str).toMatch(
      /export class Foo\s*\{\s*public get title\(\): string \{\s*return this\._title;\s*\}\s*public set title\(value: string\) \{\s*this\._title = value;\s*\}\s*\}/
    )
  });
});