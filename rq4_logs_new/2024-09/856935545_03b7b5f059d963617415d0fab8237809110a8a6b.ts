import { combineTransactionSteps, Extension, findChildrenInRange, getChangedRanges } from '@tiptap/core'
import { Plugin, PluginKey } from '@tiptap/pm/state'
import { nanoid } from 'nanoid'
import type { GlobalAttributes } from '@tiptap/core'
import type { Transaction } from '@tiptap/pm/state'

export interface SymbolOptions {
  enableName: boolean
  enableId: boolean
}

export const symbol = Extension.create<SymbolOptions>({
  name: 'symbol',
  addOptions() {
    return {
      enableName: true,
      enableId: true,
    }
  },
  addGlobalAttributes() {
    const attrs: GlobalAttributes = []

    if (this.options.enableName) {
      attrs.push(...(this.extensions.map(ext => ({
        types: [ext.name],
        attributes: {
          name: {
            default: ext.name,
            renderHTML(attributes) {
              return {
                'data-name': attributes['name'],
              }
            },
            parseHTML(element) {
              return element.getAttribute('data-name')
            },
          },
        },
      })) as GlobalAttributes))
    }

    if (this.options.enableId) {
      attrs.push(...(this.extensions.filter(ext => ext.type === 'node').map(ext => ({
        types: [ext.name],
        attributes: {
          id: {
            default: null,
            renderHTML(attributes) {
              return {
                'data-id': attributes['id'],
              }
            },
            parseHTML(element) {
              return element.getAttribute('data-id')
            },
          },
        },
      })) as GlobalAttributes))
    }

    return attrs
  },
  onCreate() {
    if (!this.options.enableId) {
      return
    }

    const dispatch = this.editor.view.dispatch
    const { tr, doc } = this.editor.view.state

    doc.descendants((node, pos) => {
      if (!node.isText && !node.attrs['id']) {
        tr.setNodeAttribute(pos, 'id', nanoid())
      }
    })

    dispatch(tr)
  },
  addProseMirrorPlugins() {
    if (!this.options.enableId) {
      return []
    }

    return [
      new Plugin({
        key: new PluginKey(this.name),
        appendTransaction(trs, { doc: oldDoc }, { doc: newDoc, tr }) {
          if (!trs.find(tr => tr.docChanged) || newDoc.eq(oldDoc)) {
            return
          }

          const transform = combineTransactionSteps(oldDoc, trs as Transaction[])
          getChangedRanges(transform).forEach(({ newRange }) => {
            const newNodes = findChildrenInRange(newDoc, newRange, node => !node.isText)
            const nodeIds = new Set<string>()

            newNodes.forEach(({ node, pos }) => {
              tr.setNodeAttribute(pos, 'name', node.type.spec.attrs?.['name']?.default)

              if (!node.attrs['id']) {
                tr.setNodeAttribute(pos, 'id', nanoid())
                return
              }

              if (nodeIds.has(node.attrs['id'])) {
                tr.setNodeAttribute(pos, 'id', nanoid())
                return
              }

              nodeIds.add(node.attrs['id'])
            })
          })

          return tr
        },
      }),
    ]
  },
})