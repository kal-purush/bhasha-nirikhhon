import { IOptionForTree } from "@/assets/types";
import BinaryOptionTree from "../BinaryOptionTree";
import BinaryTreeNode from "../BinaryTreeNode";

import NumberNode from '../components/NumberNode.vue'
import { ITreesSettings, treesFunctions } from "../types";
import STree from "./STree";

type VTreeFunction = (option: IOptionForTree, node: BinaryTreeNode<number>) => number

export default class VTreeEuropean extends BinaryOptionTree<number> {
    public dataComponent = NumberNode;

    public generate(option: IOptionForTree, STree: STree, settings: ITreesSettings): this {
        const STreeLayers = STree.layered.reverse()
        const STreeLastLayer = STreeLayers[0]
        const { r, u, d } = option
        const p = (r - d) / (u - d)

        const VTreeLastLayer = STreeLastLayer.map(node => 
            new BinaryTreeNode(functionsStorage.get(settings.function)?.call(this, option, node))
        )

        const VTreeLayers = [VTreeLastLayer]

        for (let layerIndex = 1; layerIndex < STreeLayers.length; ++layerIndex) {
            VTreeLayers.push(
                STreeLayers[layerIndex].map((node, nodeIndex) => {
                    const upChildNode = VTreeLayers[layerIndex - 1][nodeIndex * 2] as BinaryTreeNode<number>
                    const downChildNode = VTreeLayers[layerIndex - 1][nodeIndex * 2 + 1] as BinaryTreeNode<number>
    
                    const data = 1 / (1 + r) * (p * upChildNode.data + (1 - p) * downChildNode.data)
                    const parentNode = new BinaryTreeNode<number>(data)
                    parentNode.upChildren = upChildNode
                    parentNode.downChildren = downChildNode
                    return parentNode
                })
            )
        }

        this.root = VTreeLayers.reverse()[0][0] as BinaryTreeNode<number>
        return this;
    }
}

/** TODO
 *  Make as class
 */
export const functionsStorage = new Map<treesFunctions, VTreeFunction>()

functionsStorage.set(treesFunctions.func1, (option, node) => {
    const result = node.data - option.K
    return result > 0 ? result : 0
})

functionsStorage.set(treesFunctions.func2, (option, node) => {
    const result = option.K - node.data
    return result > 0 ? result : 0
})

functionsStorage.set(treesFunctions.func3, (option, node) => {
    const result = STree.S_(node) - option.K
    return result > 0 ? result : 0
})

functionsStorage.set(treesFunctions.func4, (option, node) => {
    const result = STree.S_(node) - node.data
    return result > 0 ? result : 0
})