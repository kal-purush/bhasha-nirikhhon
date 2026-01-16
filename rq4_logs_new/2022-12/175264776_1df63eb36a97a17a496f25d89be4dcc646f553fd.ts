interface DataItem {
  relationship?: string;
  causalEffect?: string;
  directness?: string;
  edge: string;
}

class DecisionTreeNode {
  attribute: string;
  value: string;
  edge: string | undefined;
  children: DecisionTreeNode[];

  constructor(attribute: string, value: string, edge?: string, children?: DecisionTreeNode[]) {
    this.attribute = attribute;
    this.value = value;
    this.edge = edge;
    this.children = children || [];
  }
}

class DecisionTree {
  root: DecisionTreeNode;

  constructor(root: DecisionTreeNode) {
    this.root = root;
  }

  getEdge(relationship: string, causalEffect?: string, directness?: string): string | undefined {
    function findEdge(node: DecisionTreeNode, relationship: string, causalEffect?: string, directness?: string): string | undefined {
      if (!node) {
        return undefined;
      }
      if (node.edge) {
        return node.edge;
      }
      if (node.attribute === "relationship") {
        for (const child of node.children) {
          if (child.value === relationship) {
            return findEdge(child, relationship, causalEffect, directness);
          }
        }
      } else if (node.attribute === "causalEffect") {
        for (const child of node.children) {
          if (child.value === causalEffect) {
            return findEdge(child, relationship, causalEffect, directness);
          }
        }
      } else if (node.attribute === "directness") {
        for (const child of node.children) {
          if (child.value === directness) {
            return findEdge(child, relationship, causalEffect, directness);
          }
        }
      }
      return undefined;
    }
    return findEdge(this.root, relationship, causalEffect, directness);
  }
}

function buildDecisionTree(data: DataItem[]): DecisionTreeNode {
  const nodeMap: Map<string, DecisionTreeNode> = new Map();
  for (const item of data) {
    const node = new DecisionTreeNode("", "", item.edge);
    nodeMap.set(item.edge, node);
    let parent: DecisionTreeNode | undefined = undefined;
    if (item.directness) {
      parent = nodeMap.get(item.directness);
      if (!parent) {
        parent = new DecisionTreeNode("directness", item.directness);
        nodeMap.set(item.directness, parent);
      }
    }
    if (item.causalEffect) {
      parent = nodeMap.get(item.causalEffect);
      if (!parent) {
        parent = new DecisionTreeNode("causalEffect", item.causalEffect);
        nodeMap.set(item.causalEffect, parent);
      }
    }
    if (item.relationship) {
      parent = nodeMap.get(item.relationship);
      if (!parent) {
        parent = new DecisionTreeNode("relationship", item.relationship);
        nodeMap.set(item.relationship, parent);
      }
    }
    if (parent) {
      parent.children.push(node);
    }
  }
  const rootNodes: DecisionTreeNode[] = [];
  for (const [key, value] of nodeMap) {
    if (!value.attribute) {
      rootNodes.push(value);
    }
  }
  return new DecisionTreeNode("", "", undefined, rootNodes);
}

// Build the decision tree using the provided data
const data = [
  {
    "relationship": "noctuaFormConfig.activityRelationship.options.constitutivelyUpstream.name",
    "edge": "noctuaFormConfig.edge.constitutivelyUpstreamOf"
  },
  // ... other data items
];

function getEdge(relationship: string, causalEffect?: string, directness?: string): string | undefined {
  const tree = dataToTree(data);
  if (tree[relationship]) {
    if (tree[relationship].edge) {
      return tree[relationship].edge;
    } else if (causalEffect && tree[relationship][causalEffect]) {
      if (tree[relationship][causalEffect].edge) {
        return tree[relationship][causalEffect].edge;
      } else if (directness && tree[relationship][causalEffect][directness]) {
        return tree[relationship][causalEffect][directness].edge;
      }
    }
  }
  return undefined;
}