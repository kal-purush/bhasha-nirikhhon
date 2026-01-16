import { defineCommand } from 'citty'
import { createPrinter, printerTypeOrThrow } from '../printer'
import { consola } from 'consola'
import { colorize } from 'consola/utils'
import { readDirDeepSyncAsPaths, setupComponentRegistry } from '../registry'
import { sharedArgs } from './_shared'
import { GraphLoader } from '../graph-loader'
import { Node } from '../node'
import { vueFileNameOrThrow } from '../util'

export default defineCommand({
  meta: {
    name: 'load-all',
    description: 'Load all dependency graphs from a directory.'
  },
  args: {
    viewsDir: {
      type: 'positional',
      required: true,
      description: 'Root directory to load. Vurif recognizes entries of this as starting points of graph'
    },
    componentsDir: {
      type: 'positional',
      required: true,
      description: '"vurif" target directory to load'
    },
    ...sharedArgs
  },
  run({ args }) {
    const viewsDir = args.viewsDir
    const componentsDir = args.componentsDir
    const format = args.format === 'stdout' ? 'stdout' : 'graph'
    // Early check as validation
    const printerType = printerTypeOrThrow(format)

    consola.info(
      `Traversing "${colorize('blue', viewsDir)}" associated with "${colorize('blue', componentsDir)}"`
    )

    const registry = setupComponentRegistry(componentsDir)    
    const loader = new GraphLoader(registry)

    const rootPaths = readDirDeepSyncAsPaths(viewsDir)
    const rootNodes: Node[] = []
    for (const entry of rootPaths) {
      // Set all view paths to the registry to look self up from it
      // Every root nodes will look up itself from the registry, so keep it registered here
      registry.set(entry, vueFileNameOrThrow(entry))
      const rootNode = new Node(entry)
      loader.load(rootNode)
      rootNodes.push(rootNode)
    }

    createPrinter(registry, printerType)
      .onCompleted(() => {
        consola.success(colorize('green', `Analysis succeeded for: ${viewsDir}`))
      })
      .printAll(rootNodes)
  },
})