import type { VueFileName } from '../util'
import type { Node } from '../node'
import type { ElementDefinition } from 'cytoscape'
import type { ComponentRegistry } from '../registry'

interface NodeDef {
  id: string
  name: VueFileName
}

export interface EdgeDef {
  id: string,
  source: VueFileName,
  target: VueFileName
}

export function isEdgeDef(element: GraphElement): element is EdgeDef {
  const props = Object.keys(element)
  return props.includes('source') && props.includes('target')
}

/**
 * Custom element definitions
 */
export type GraphElement = NodeDef | EdgeDef

type ElementType = 'cytoscape' | 'object'
type ElementTypeOf<S extends ElementType> = S extends 'cytoscape'
  ? ElementDefinition // cytoscape type definition
  : GraphElement // custom definition

interface GraphStrategy<T> {
  /**
   * Create data representing a node
   */
  createNodeDef(name: VueFileName): T

  /**
   * Create data representing a edge connecting nodes
   */
  createEdgeDef(source: VueFileName, target: VueFileName): T

  /**
   * Get id from a graph element
   */
  getId(element: T): string
}

export class CytoscapeGraphStrategy implements GraphStrategy<ElementDefinition> {
  createNodeDef(name: VueFileName): ElementDefinition {
    return {
      data: { id: name }
    }
  }

  createEdgeDef(source: VueFileName, target: VueFileName): ElementDefinition {
    return {
      data: {
        id: `${source}-${target}`,
        source,
        target
      }
    }
  }

  getId(element: ElementDefinition): string {
    return element.data.id ?? ''
  }
}

export class ObjectGraphStrategy implements GraphStrategy<GraphElement> {
  createNodeDef(name: VueFileName): GraphElement {
    return {
      id: name,
      name
    }
  }

  createEdgeDef(source: VueFileName, target: VueFileName): GraphElement {
    return {
      id: `${source}-${target}`,
      source,
      target
    }
  }

  getId(element: GraphElement): string {
    return element.id
  }
}

/**
 * Dependency resolution cache.
 * NOTE: Element equality in a cache is specified by `id`
 */
class ResolutionCache<E extends ElementType> {
  private readonly resolvedIds: Set<string> = new Set()

  constructor(private strategy: GraphStrategy<ElementTypeOf<E>>) {}

  markAsResolved (...elms: ElementTypeOf<E>[]): void {
    for (const elm of elms) {
      const id = this.strategy.getId(elm)
      this.resolvedIds.add(id)
    }
  }

  alreadyResolved(elm: ElementTypeOf<E>): boolean {
    const id = this.strategy.getId(elm)
    return this.resolvedIds.has(id)
  }
}

/**
 * Graph generator creating with output strategy
 * FIXME: redundant parameter(strategy constructor same as type parameter)
 */
export class GraphGenerator<E extends ElementType> {
  private graphStrategy: GraphStrategy<ElementTypeOf<E>>

  constructor(private registry: ComponentRegistry, strategyConstructor: new () => GraphStrategy<ElementTypeOf<E>>) {
    this.graphStrategy = new strategyConstructor()
  }

  /**
   * Traverse all nodes and skip it when once resolved
   */
  generateAll(nodes: Node[], cache: ResolutionCache<E> = new ResolutionCache(this.graphStrategy)): ElementTypeOf<E>[] {
    const createChildAndEdge = (parent: Node, child: Node): ElementTypeOf<E>[] => {
      const parentName = this.registry.get(parent.name)
      const childName = this.registry.get(child.name)
      // An edge from parent to child is always new
      // Skip resolving when child node has been resolved once or more, and children are maybe blank.
      const childNodeDefs = this.generateAll([child], cache)
      const edgeDef = this.graphStrategy.createEdgeDef(parentName, childName)

      return [edgeDef, ...childNodeDefs]
    }

    const elements = []
    for (const node of nodes) {
      const name = this.registry.get(node.name)
      const nodeDef = this.graphStrategy.createNodeDef(name)
      if (cache.alreadyResolved(nodeDef)) continue
      if (!node.hasEdges()) {
        cache.markAsResolved(nodeDef)
        elements.push(nodeDef)
        continue
      }

      // Create child node and edge from parent to child
      const childNodeDefs = Object.entries(node.edges).flatMap(
        (edge: [_: string, n: Node]) => (createChildAndEdge(node, edge[1]))
      )

      elements.push(nodeDef, ...childNodeDefs)
      cache.markAsResolved(nodeDef, ...childNodeDefs)
    }

    return elements
  }

  /**
   * Create graph elements(nodes and edges).
   */
  generate(node: Node): ElementTypeOf<E>[] {
    const name = this.registry.get(node.name)
    const nodeDef = this.graphStrategy.createNodeDef(name)

    if (!node.hasEdges()) return [nodeDef]

    const childNodeDefs = Object.entries(node.edges).map((edge: [name: string, n: Node]) => {
      const childNode = edge[1]
      // create a edge leading to child node
      const parentName = this.registry.get(node.name)
      const childName = this.registry.get(childNode.name)
      const edgeDef = this.graphStrategy.createEdgeDef(parentName, childName)
      const childNodeDefs = this.generate(childNode)

      return [edgeDef, ...childNodeDefs]
    })

    // Safe-typing for Type inference of return type
    const elements = childNodeDefs.flat()
    elements.push(nodeDef)

    return elements
  }
}