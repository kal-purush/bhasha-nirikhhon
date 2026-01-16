// Copyright (c) 2015 Vadim Macagon
// MIT License, see LICENSE file for full terms.

'use strict';

export default function ({ types }: { types: BabelTypes }) {
  return {
    visitor: {
      // Remove `extends Polymer.BaseClass()` from an ES6 class expression
      ClassExpression(path: NodePath<BabelNodeClassExpression>) {
        if (types.isCallExpression(path.node.superClass)) {
          const calleePath = path.get('superClass.callee');
          if (calleePath.matchesPattern('Polymer.BaseClass')) {
            path.get('superClass').remove();
          }
        }
      }
    }
  }
}