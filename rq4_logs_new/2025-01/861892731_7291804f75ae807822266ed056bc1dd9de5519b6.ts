import { NodeTypes, FlowWorkflowNode, FlowWorkflowEdge, FlowWorkflowNodeConfig } from '@/types/api_types/nodeTypeSchemas';
import { TestInput } from '@/types/api_types/workflowSchemas';


export interface FlowState {
    nodeTypes: NodeTypes
    nodes: FlowWorkflowNode[]
    edges: FlowWorkflowEdge[]
    nodeConfigs: Record<string, FlowWorkflowNodeConfig>
    workflowID: string | null
    selectedNode: string | null
    sidebarWidth: number
    projectName: string
    workflowInputVariables: Record<string, any>
    testInputs: TestInput[]
    inputNodeValues: Record<string, any>
    history: {
        past: Array<{ nodes: FlowWorkflowNode[]; edges: FlowWorkflowEdge[]} >
        future: Array<{ nodes: FlowWorkflowNode[]; edges: FlowWorkflowEdge[]} >
    }
}