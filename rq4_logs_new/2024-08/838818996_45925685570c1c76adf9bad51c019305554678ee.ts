import { describe, it, expect, beforeEach } from 'vitest'
import { ConsolePrinter } from '../../src/printers'
import { MockComponentRegistry } from '../test-utils'
import { Node } from '../../src/node'

describe('ConsolePrinter#createLines', () => {
  const root = new Node('pages/index.vue')

  describe('when a graph has a root node', () => {
    const registry = new MockComponentRegistry({
      'pages/index.vue': 'pages/index.vue'
    })
    const printer = new ConsolePrinter(registry)

    it('should format no paddings', () => {
      const lines = printer.createLines(root)

      expect(lines).toHaveLength(1)
      expect(lines[0]).toBe('pages/index.vue')
    })
  })

  describe('when a graph depth is 2', () => {
    const registry = new MockComponentRegistry({
      'pages/index.vue': 'pages/index.vue',
      'App': 'components/App.vue',
    })
    const printer = new ConsolePrinter(registry)
    beforeEach(() => {
      root.addEdges({
        App: new Node('App')
      })
    })

    it('should return an array containing formatted line', () => {
      const lines = printer.createLines(root)

      expect(lines).toHaveLength(2)
      expect(lines[0]).toBe('pages/index.vue')
      expect(lines[1]).toBe('  └── components/App.vue')
    })
  })

  describe('when a graph has some nests', () => {
    const registry = new MockComponentRegistry({
      'pages/index.vue': 'pages/index.vue',
      'App': 'components/App.vue',
      'Button': 'components/Button.vue',
      'Header': 'components/Header.vue',
      'Message': 'components/Message.vue',
    })
    const printer = new ConsolePrinter(registry)
    beforeEach(() => {
      root.addEdges({
        App: new Node('App', {
          Header: new Node('Header', {
            Button: new Node('Button')
          }),
          Message: new Node('Message')
        })
      })
    })

    it('should return an array containing formatted lines', () => {
      const lines = printer.createLines(root)

      expect(lines).toHaveLength(5)
      expect(lines[0]).toBe('pages/index.vue')
      expect(lines[1]).toBe('  └── components/App.vue')
      expect(lines[2]).toBe('    └── components/Header.vue')
      expect(lines[3]).toBe('      └── components/Button.vue')
      expect(lines[4]).toBe('    └── components/Message.vue')
    })
  })
})