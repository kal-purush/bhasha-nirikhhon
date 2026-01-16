function createTreeNode(index: number, log: ILog, failToGetParent: boolean, parent: ITreeNode): ITreeNode {
    var node: ITreeNode = {
        index: index,
        log: log,
        failToGetParent: failToGetParent,
        parent: parent,
        nodes: []
    };

    return node;
}


function getParent(log: ILog, currentParent: ITreeNode): ITreeNode {
    if (currentParent.index === -1) return null;

    if (log.info.objectName.toLowerCase() === currentParent.log.object_name.toLowerCase()) {
        return currentParent;
    }
    for (var i = 0; i < currentParent.parent.nodes.length; i++) {
        var parent = currentParent.parent.nodes[i];
        if (log.info.objectName === parent.log.object_name) {
            return parent;
        }
    }

    return getParent(log, currentParent.parent);
}

export function build(logs: ILog[]): ITreeNode {

    var root: ITreeNode = {
        index: -1,
        log: null,
        failToGetParent: true,
        parent: null,
        nodes: []
    },
        currentNode: ITreeNode = null;

    logs.forEach((log, index) => {
        if (currentNode) {
            if (log.info.objectName) {
                var parent: ITreeNode = getParent(log, currentNode);

                if (parent) {
                    currentNode = createTreeNode(index, log, true, parent);
                    parent.nodes.push(currentNode);
                } else {
                    // throw "cannot find parent";
                    console.log("cannot find parent", index);
                    root.nodes.push(createTreeNode(index, log, false, root));
                }
            } else {
                root.nodes.push(createTreeNode(index, log, true, root));
            }
        } else {
            currentNode = createTreeNode(index, log, true, root);
            root.nodes.push(currentNode);
        }
    });

    return root;
}