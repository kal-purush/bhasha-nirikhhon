import { Scene, VertexData } from 'babylonjs';

import Shape from './Shape';

/**
 * Properties of a box.
 */
export interface IBoxProps {
  size: number;
  width: number;
  height: number;
  depth: number;
}

/**
 * A box mesh with updatable vertices.
 */
export default class Box extends Shape<IBoxProps> {
  private static readonly DEFAULT_PROPS: IBoxProps = {
    size: 1,
    width: 1,
    height: 1,
    depth: 1
  };

  constructor(scene: Scene, name?: string, props?: Partial<IBoxProps>) {
    super(
      scene,
      name ?? 'Box',
      { ...Box.DEFAULT_PROPS, ...props },
      VertexData.CreateBox
    );
  }

  public get size(): number {
    return this.props.size;
  }

  public set size(value: number) {
    this.update({ size: value });
  }

  public get width(): number {
    return this.props.width;
  }

  public set width(value: number) {
    this.update({ width: value });
  }

  public get height(): number {
    return this.props.height;
  }

  public set height(value: number) {
    this.update({ height: value });
  }

  public get depth(): number {
    return this.props.depth;
  }

  public set depth(value: number) {
    this.update({ depth: value });
  }
}