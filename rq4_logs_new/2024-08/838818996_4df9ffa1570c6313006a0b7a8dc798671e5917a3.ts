import { Node } from '../src/node'
import { createPrinter } from '../src/printer'
import { DefaultComponentsRegistry } from '../src/registry'
import { vueFileNameOrThrow } from '../src/util'
import { VueFile } from '../src/vue-file'
import { GraphLoader } from '../src/graph-loader'
import { consola } from 'consola'

const fileName = process.argv[2]
const componentsDir = process.argv[3]
const vueFileName = vueFileNameOrThrow(fileName)
consola.start(`Start explore "${vueFileName}" dependencies from a directory "${componentsDir}".`)

const registry = new DefaultComponentsRegistry(componentsDir)
const vueFile = VueFile.fromOriginal(fileName)
registry.set(vueFile.componentKey, vueFileName)

const rootNode = new Node(vueFile.componentKey)
new GraphLoader(registry).load(rootNode)

createPrinter(registry, 'graph')
  .onCompleted(() => {
    consola.success(`End explore "${vueFileName}" dependencies from a directory "${componentsDir}".`)
  })
  .print(rootNode)