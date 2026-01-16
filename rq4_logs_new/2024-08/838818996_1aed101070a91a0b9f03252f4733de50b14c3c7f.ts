import { describe, it, expect, afterEach, beforeEach } from 'vitest'
import mockFs from 'mock-fs'
import { GraphLoader } from '../src/graph-loader'
import { Node } from '../src/node'
import { MockComponentRegistry } from './test-utils'

afterEach(() => {
  mockFs.restore()
})

describe('GraphLoader', () => {
  describe('when the node name does not map to any element', () => {
    const registry = new MockComponentRegistry({})
    const loader = new GraphLoader(registry)
    beforeEach(() => {
      mockFs({})
    })

    it('should load no edges', () => {
      const root = new Node('pages/index.vue')
      loader.load(root)

      expect(root.edges).toStrictEqual({})
    })
  })

  describe('when a root node uses no components', () => {
    const registry = new MockComponentRegistry({
      'pages/index.vue': 'pages/index.vue'
    })
    const loader = new GraphLoader(registry)
    beforeEach(() => {
      mockFs({
        'pages': {
          'index.vue': '<template></template>'
        }
      })
    })

    it('should load no edges', () => {
      const root = new Node('pages/index.vue')
      loader.load(root)

      expect(root.edges).toStrictEqual({})
    })
  })

  describe('when a root uses a component', () => {
    const registry = new MockComponentRegistry({
      'App': 'components/App.vue',
      'pages/index.vue': 'pages/index.vue'
    })
    const loader = new GraphLoader(registry)
    beforeEach(() => {
      mockFs({
        'components': {
          'App.vue': '<template></template>'
        },
        'pages': {
          'index.vue': '<template><App /></template>'
        }
      })
    })

    it('should load a edge', () => {
      const root = new Node('pages/index.vue')
      loader.load(root)

      expect(root.edges.App).toBeDefined()
      expect(root.edges.App.edges).toStrictEqual({})
    })
  })

  describe('when a root node uses nested components', () => {
    const registry = new MockComponentRegistry({
      'App': 'components/App.vue',
      'Button': 'components/atoms/Button.vue',
      'pages/index.vue': 'pages/index.vue'
    })
    const loader = new GraphLoader(registry)
    beforeEach(() => {
      mockFs({
        'components': {
          'App.vue': '<template><Button>save</Button></template>',
          'atoms': {
            'Button.vue': '<template><button type="button"><slot /></button></template>'
          }
        },
        'pages': {
          'index.vue': '<template><App /></template>'
        }
      })
    })

    it('should load nested edges', () => {
      const root = new Node('pages/index.vue')
      loader.load(root)

      expect(root.edges.App).toBeDefined()
      expect(root.edges.App.edges.Button).toBeDefined()
      expect(root.edges.App.edges.Button.edges).toStrictEqual({})
    })
  })

  describe('when a component has v-if condition', () => {
    const registry = new MockComponentRegistry({
      'Button': 'components/atoms/Button.vue',
      'App': 'components/App.vue',
      'AddButton': 'components/AddButton.vue',
      'DeleteButton': 'components/DeleteButton.vue',
      'UpdateButton': 'components/UpdateButton.vue',
      'pages/index.vue': 'pages/index.vue'
    })
    const loader = new GraphLoader(registry)
    beforeEach(() => {
      mockFs({
        'components': {
          'App.vue': `
<template>
  <section>
    <AddButton v-if="showAddButton" />
    <UpdateButton v-else-if="showUpdateButton" />
    <DeleteButton v-else />
  </section>
</template>`,
          'AddButton.vue': '<template><Button>add</Button></template>',
          'DeleteButton.vue': '<template><Button>delete</Button></template>',
          'UpdateButton.vue': '<template><Button>update</Button></template>',
          'atoms': {
            'Button.vue': '<template><button type="button"><slot /></button></template>'
          }
        },
        'pages': {
          'index.vue': '<template><App /></template>'
        }
      })
    })

    it('should load nested edges', () => {
      const root = new Node('pages/index.vue')
      loader.load(root)

      expect(root.edges.App).toBeDefined()
      expect(root.edges.App.edges.AddButton).toBeDefined()
      expect(root.edges.App.edges.AddButton.edges.Button).toBeDefined()
      expect(root.edges.App.edges.DeleteButton).toBeDefined()
      expect(root.edges.App.edges.DeleteButton.edges.Button).toBeDefined()
      expect(root.edges.App.edges.UpdateButton).toBeDefined()
      expect(root.edges.App.edges.UpdateButton.edges.Button).toBeDefined()
    })
  })
})