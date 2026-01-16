export default function(babel) {
  const { types: t } = babel
  return {
    visitor: {
      CallExpression(path) {
        const originalExpression = path.findParent(t.isExpressionStatement)
        if (!originalExpression) {
          return
        }
        originalExpression.node.expression.arguments = originalExpression.node.expression.arguments.map(
          argument => buildMemberExpression(argument.name)
        )
        const program = path.findParent(t.isProgram)
        const expressionIndex = program.node.body.indexOf(
          originalExpression.node
        )
        const callExpression = buildEffectCallExpression(
          originalExpression.node
        )
        const expressionStatement = t.expressionStatement(callExpression)
        program.node.body.splice(expressionIndex, 1, expressionStatement)
      },
      VariableDeclaration(path) {
        const newDeclarations = path.node.declarations.map(declaration => {
          const result = t.variableDeclarator(
            declaration.id,
            buildObservableExpression(declaration.init)
          )
          return result
        })
        path.node.declarations = newDeclarations
      },
      BinaryExpression(path) {
        const leftIsVariable = looksLike(path.node, {
          left: {
            type: 'Identifier',
          },
        })
        const rightIsVariable = looksLike(path.node, {
          right: {
            type: 'Identifier',
          },
        })
        if (!leftIsVariable && !rightIsVariable) {
          return
        }
        if (leftIsVariable) {
          const name = path.node.left.name
          path.node.left = buildMemberExpression(name)
        } else {
          const name = path.node.right.name
          path.node.right = buildMemberExpression(name)
        }
      },
      ExpressionStatement(path) {
        if (
          !looksLike(path.node, {
            expression: {
              name: {},
            },
          })
        ) {
          return
        }
        const name = path.node.expression.name
        path.node.expression = buildMemberExpression(name)
      },
    },
  }

  function buildObservableExpression(value) {
    return t.callExpression(t.identifier('observable'), [
      t.arrowFunctionExpression([], value),
    ])
  }

  function buildEffectCallExpression(node) {
    return t.callExpression(t.identifier('effect'), [
      t.arrowFunctionExpression([], t.blockStatement([node], [])),
    ])
  }

  function buildMemberExpression(name) {
    return t.memberExpression(t.identifier(name), t.identifier('value'))
  }
}

function looksLike(a, b) {
  return (
    a &&
    b &&
    Object.keys(b).every(bKey => {
      const bVal = b[bKey]
      const aVal = a[bKey]
      if (typeof bVal === 'function') {
        return bVal(aVal)
      }
      return isPrimitive(bVal) ? bVal === aVal : looksLike(aVal, bVal)
    })
  )
}

function isPrimitive(val) {
  return val == null || /^[sbn]/.test(typeof val)
}