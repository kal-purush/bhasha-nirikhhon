/**
 * Created by Administrator on 2016/8/10.
 */
import React,{Component } from 'react'

export default class TreeNode extends Component {
    render() {
        const nodeListDom = []
        const node = this.props.node
        for(let nodeId of this.props.node.childIds){
            var childNode = node.getNode(nodeId)
            nodeListDom.push(<TreeNode node={childNode}/>)
        }
        return (<div>
                    <h3>{node.data('title')}</h3>
                    <ul>
                        {nodeListDom}
                    </ul>
                </div>
            )
    }
}