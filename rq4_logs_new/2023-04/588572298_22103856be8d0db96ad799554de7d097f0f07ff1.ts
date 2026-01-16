import { template, types } from "babel-core";
import {TranspilationError} from "../TranspilationError";

export class NodeReleaser {
    protected readonly node: types.Expression;

    constructor(node: types.Node) {
        if (!types.isExpression(node)) {
            throw new TranspilationError(
                `Trying to release node, but ${node.type} is not Expression.`,
            );
        }
        this.node = node;
    }

    public static released(node: types.Node) {
        return new this(node).released();
    }

    public released() {
        return Nodes.releaser(this.node);
    }

}

namespace Templates {
    export type Releaser = (nodes: {
        target: types.Expression;
    }) => types.ExpressionStatement;

    export const releaser = <Releaser>(
        (<{}>template("taintflow.Flow.of(target).release()"))
    );
}

namespace Nodes {
    export function releaser(target: types.Expression) {
        return Templates.releaser({ target: target})
    }
}