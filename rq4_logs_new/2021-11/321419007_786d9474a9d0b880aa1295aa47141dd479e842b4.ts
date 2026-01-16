import { GraphModel } from '../GraphModel'

export interface ModelItem {
  id: string
  attributes: ModelAttributeSet
}

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface ModelNode extends ModelItem, Partial<NodeDecoration> {}

export interface ModelEdge extends ModelItem, Partial<EdgeDecoration> {
  source: string
  target: string
}

export type ModelAttributeSet = Record<string, ModelAttributeValue>

export type ModelAttributeValue = unknown

export type ModelAttributeTypes = Record<string, Set<string>>

export type ModelGraphData = {
  nodes: Record<string, ModelNode>
  edges: Record<string, ModelEdge>
}

export interface ItemDecoration {
  label?: string
  size: number
  color: string
  opacity: number
}

export interface NodeDecoration extends ItemDecoration {
  icon?: string
  shape: NodeShape
  strokeColor: string
  strokeSize: number
  // label opts
}

export interface EdgeDecoration extends ItemDecoration {
  sourceArrow: boolean
  targetArrow: boolean
  style: EdgeStyle
}

export interface DecorationFunction<T extends ItemDecoration, S = Partial<T>> {
  (): S
}

export type NodeDecorationFunction = DecorationFunction<NodeDecoration>
export type EdgeDecorationFunction = DecorationFunction<EdgeDecoration>

export interface NodeDecorationCreator {
  getDecorationOverrides: NodeDecorationFunction
}

export interface EdgeDecorationCreator {
  getDecorationOverrides: EdgeDecorationFunction
}

export type DecoratedNode = ModelNode & NodeDecorationCreator

export type DecoratedEdge = ModelEdge & EdgeDecorationCreator

export type NodeDecorator = {
  (node: ModelNode): Partial<NodeDecoration>
  id?: string
}

export type EdgeDecorator = {
  (edge: ModelEdge): Partial<EdgeDecoration>
  id?: string
}

export type NodeShape = string

export type EdgeStyle = string

export interface CustomGraphLayout {
  name: string
  runLayout(
    model: GraphModel,
    options: GraphLayoutOptions
  ): Record<string, NodePosition>
  // called on continuous layouts to stop them before they finish
  stopLayout(): void
}

export type NodePosition = {
  x: number
  y: number
}

export interface GraphLayoutOptions {
  boundingBox: BoundingBox
}

export interface BoundingBox {
  x1: number
  y1: number
  w: number
  h: number
}

export type GraphCommand =
  | ZoomInCommand
  | ZoomOutCommand
  | RefitCommand
  | LayoutCommand

export interface ZoomInCommand {
  type: 'ZoomIn'
}

export interface ZoomOutCommand {
  type: 'ZoomOut'
}

export interface RefitCommand {
  type: 'Refit'
}

export interface LayoutCommand {
  type: 'Layout'
}

/// JSON GRAPH Interfaces - only describes what we need from
/// https://github.com/jsongraph/json-graph-specification/blob/master/json-graph-schema_v2.json
/// Note we do not support hyperedges

export interface JSONGraphNode {
  label?: string
  metadata?: Record<string, unknown>
}

export interface JSONGraphEdge {
  id?: string
  source: string
  target: string
  relation?: string
  directed?: boolean
  label?: string
  metadata?: Record<string, unknown>
}

export interface JSONGraph {
  nodes: Record<string, JSONGraphNode>
  edges: JSONGraphEdge[]
}

export interface JSONGraphData {
  graph?: JSONGraph
  graphs?: JSONGraph[]
}