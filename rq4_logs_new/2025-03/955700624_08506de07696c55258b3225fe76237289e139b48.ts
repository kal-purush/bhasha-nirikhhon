import { FamilyNode } from '@/features/family-tree/data/family';

class FamilyTreeData {
    private static instance: FamilyTreeData;
    private nodes: FamilyNode[] = [];

    private constructor() {}

    public static getInstance(): FamilyTreeData {
        if (!FamilyTreeData.instance) {
            FamilyTreeData.instance = new FamilyTreeData();
        }
        return FamilyTreeData.instance;
    }

    public setNodes(nodes: FamilyNode[]): void {
        this.nodes = nodes;
    }

    public getNodes(): FamilyNode[] {
        return this.nodes;
    }

    public addNode(node: FamilyNode): void {
        this.nodes.push(node);
    }

    public updateNode(updatedNode: FamilyNode): void {
        this.nodes = this.nodes.map((node) => (node.id === updatedNode.id ? { ...node, ...updatedNode } : node));
    }

    public getNodeById(id: string): FamilyNode | undefined {
        return this.nodes.find((node) => node.id === id);
    }
}


export { FamilyTreeData };