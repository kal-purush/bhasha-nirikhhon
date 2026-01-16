import Block from "./Block";
import BlockVisitor from "./BlockVisitor";

export default class NumberOutputBlock extends Block {
  public readonly numInputs = 1;
  public readonly numOutputs = 0;
  constructor() {
    super();
  }

  accept<R>(blockVisitor: BlockVisitor<R>): R {
    return blockVisitor.visitNumberOutputBlock(this);
  }
}