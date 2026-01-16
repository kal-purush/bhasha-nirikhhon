import { NodeCreator } from './NodeCreator';
import NodeRemover from './NodeRemover';

export abstract class Node {

    private parent:Node|null;

    constructor(protected creator: NodeCreator, protected remover: NodeRemover) {

    }


    setCreator(creator: NodeCreator) {
        this.creator = creator;
    }

    setRemover(remover: NodeRemover) {
        this.remover = remover;
    }


    setParent(node:Node){
        this.parent= node;
    }

    getParent(){
        return this.parent;
    }

    abstract GetChildren(): Promise<Node[]>;

    abstract CreateSubKey(information: any): Promise<boolean>;

    abstract DeleteSubKey(information: any): Promise<boolean>;

}


export default Node;