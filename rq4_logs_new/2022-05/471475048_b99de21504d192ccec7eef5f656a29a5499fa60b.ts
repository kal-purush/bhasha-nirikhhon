import { IOptionForTree } from "@/assets/types";
import BinaryOptionTree from "../BinaryOptionTree";
import BinaryTreeNode from "../BinaryTreeNode";

import NumberNode from '../components/NumberNode.vue'

export default class STree extends BinaryOptionTree<number> {
    public dataComponent = NumberNode;
    
    generate(option: IOptionForTree, layers: number): this {
        this.root = new BinaryTreeNode<number>(option.S)
        
        this.root.upChildren = new BinaryTreeNode(1)
        this.root.downChildren = new BinaryTreeNode(1)

        this.BFS((node) => {
            if (node.layer < layers) {
                const upData = (1 + option.u) * node.data
                const downData = (1 + option.d) * node.data
                node.upChildren = new BinaryTreeNode(upData)
                node.downChildren = new BinaryTreeNode(downData)
            }
        })

        return this
    }
}