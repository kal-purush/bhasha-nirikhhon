const DELIMITER_LEN = 2
const DELIMITER = ' '

class CycleViolationInTreeError extends Error {}

export class EventNode {
    readonly name: String;
    outcomes = new Map();

    constructor(name: String){
        this.name = name;
    }
    
    add(newEvent: EventNode){
        this.invariant(newEvent);
        this.outcomes.set(newEvent.name, newEvent);
    }

    remove(newEvent: String){
        this.outcomes.delete(newEvent);
    }

    generateLevels(){ // organize nodes by depth where array idx is depth, and array members are nodes at that depth
        let levels = new Array();
        this.preorder(
            (node, depth) => {
                levels[depth] = levels[depth] ? levels[depth]: new Array()
                levels[depth].push(node)
        });
        return levels;
    }

    toString(): String { // represent tree as string. shows overall structure. indentation depth for each node reflects its actual depth 
        let s = "";
        this.preorder(
            (node, depth) => {s = s.concat(`${DELIMITER.repeat(DELIMITER_LEN * depth)}${node.name}\n`)}
        );
        return s;
    }

    private invariant(recentNode: EventNode): Boolean { // what must be true after every insertion. in the case of a tree, no cycles
        let visitedNodes = new Set<EventNode>([recentNode]);
        let hasCycle = false;
        this.preorder((node, _) => { // currently not very efficient, must visit every node. TODO: make it quit upon finding a cycle
            hasCycle = visitedNodes.has(node);
            visitedNodes.add(node);
        })
        if (hasCycle) {
            throw new CycleViolationInTreeError(`${recentNode.name} has caused a cycle in ${this.name}.`);
        }
        return !hasCycle;
    }
    //TODO: make this a generator
    private preorder(apply: (atNode: EventNode, depth: number) => void){ // preorder traversal with depth tracking
        const helper = (depth: number) => {
            depth = depth ? depth : 0;
            apply(this, depth);
            if (this.outcomes.size > 0){
                for (const outcome of this.outcomes.values()) {
                    outcome.preorder(apply, depth + 1);
                }
            }
        }
        helper(0);
    }

}