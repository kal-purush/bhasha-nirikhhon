export interface IBinaryNode<DataType> {
    data: DataType;

    parent: IBinaryNode<DataType> | null;
    upChildren: IBinaryNode<DataType> | null;
    downChildren: IBinaryNode<DataType> | null;
    layer: number;
}

export default class BinaryTreeNode<DataType> implements IBinaryNode<DataType> {
    public parent: BinaryTreeNode<DataType> | null = null;
    private _upChildren: BinaryTreeNode<DataType> | null = null;
    private _downChildren: BinaryTreeNode<DataType> | null = null;

    constructor(
        public readonly data: DataType
    ) {}

    public get layer(): number {
        if (this.parent) return this.parent.layer + 1
        else return 0
    }

    public set upChildren(node: BinaryTreeNode<DataType>) {
        this._upChildren = node
        node.parent = this
    }
    public get upChildren(): BinaryTreeNode<DataType>{
        return this._upChildren as BinaryTreeNode<DataType>
    }

    public set downChildren(node: BinaryTreeNode<DataType>) {
        this._downChildren = node
        node.parent = this
    }
    public get downChildren(): BinaryTreeNode<DataType>{
        return this._downChildren as BinaryTreeNode<DataType>
    }
}