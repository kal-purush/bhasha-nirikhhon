import Logger from '../../Logger';
import {
  // BasePropsType,
} from '../../..';

import {
  Blueprint,
  IParentableBy,
  createComponent,
  BasePropsType,
  _RenderableType,
  _IParentableBy,
  BaseBlueprint,
} from './types';
import { ICommonBlueprintBase } from '../CommonBlueprintBase';

export class NoneExistentBlueprint
    extends Blueprint<BasePropsType> {

  public init(parent: Blueprint<BasePropsType>): void {}

  public reorderChildren(
    oldChildrenList: Blueprint<BasePropsType>[],
    newChildrenList: Blueprint<BasePropsType>[]
  ): void { }
  public updateBeforeChildren(props: BasePropsType): void {}
  public updateAfterChildren(props: BasePropsType): void {}
  public cleanUp(): void {}
}

export type FakeRootPropsType = {

} & BasePropsType;

export class _FakeRoot
    extends Blueprint<FakeRootPropsType>
    implements IParentableBy<NoneExistentBlueprint> {
  public someCommonMethod: () => 'hello';
  private logger: Logger<ICommonBlueprintBase>;
  constructor(logger?: Logger<ICommonBlueprintBase>) {
    super();
    this.logger = logger;
  }
  public init(parent: NoneExistentBlueprint): void { }
  public updateBeforeChildren(props: BasePropsType): void { }
  public updateAfterChildren(props: BasePropsType): void { }

  public reorderChildren(
    oldChildrenList: Blueprint<BasePropsType>[],
    newChildrenList: Blueprint<BasePropsType>[]
  ): void { }
  public cleanUp(): void { }
}

/**
 * INTERNAL USE: Fake Root
 * @param props: FakeRoot props
 */
const fakeRoot = createComponent<
  _FakeRoot,
  NoneExistentBlueprint,
  FakeRootPropsType
> (_FakeRoot);

export default fakeRoot;

export {
  _RenderableType as RenderableType,
  _IParentableBy as IParentableBy,
  BaseBlueprint,
};