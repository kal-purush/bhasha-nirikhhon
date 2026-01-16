module TreeStructures {
    export class DummyDataProvider {

        public static instanceCounter = 0;

        public static fillNode(node: TreeStructures.TreeNode<string>): JQueryPromise<string> {
            var dfd = $.Deferred();

            if (DummyDataProvider.instanceCounter < 4) {
                window.setTimeout(() => {
                    for (var i = 0; i < 3; i++) {
                        var childNode = new TreeStructures.TreeNode<string>( node.item + "/" + DummyDataProvider.instanceCounter);
                        node.addChild(childNode);
                    }
                    console.log("Resolving node: " + node.item);
                    dfd.resolve(node.item);
                }, 500 + Math.random() * 1000);
            } else {
                console.log("Resolving finale node: " + node.item);
                dfd.resolve(node.item);
            }

            return dfd.promise();
        }

        public static fillNodeFromSource(node: TreeStructures.TreeNode<string>, source: TreeStructures.TreeNode<string>): JQueryPromise<string> {
            var dfd = $.Deferred();
            var promises = [];
            window.setTimeout(() => {

                node.item = source.item;
                for (var sourceNode in source.children) {
                    var childNode = new TreeStructures.TreeNode<string>(source.children[sourceNode].item);
                    node.addChild(childNode);
                    promises.push(this.fillNodeFromSource(childNode, source.children[sourceNode]));
                }

                $.when.apply(null, promises).then(() => {
                    console.log("Resolving: " + node.item);
                    dfd.resolve(node.item);
                });
            }, 500 + Math.random() * 1000);

            return dfd.promise();
        }

    }
}