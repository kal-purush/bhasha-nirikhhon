import { visit } from 'unist-util-visit'

export default function remarkWikiUrl(options: { cdnUrl: string }) {
  return function (node) {
    visit(node, 'paragraph', (node, index, parent) => {
      const lastChild = node.children[node.children.length - 1]
      if (lastChild && lastChild.type === 'text') {
        const string = lastChild.value.replace(/ +$/, '')
        const matched = string.match(/!\[{2}(.+)?\]{2}/)

        if (matched) {
          const imageNode = {}
          const wikiLink = matched[0]
          const keys = matched[1].split('|')
          imageNode.type = 'image'
          imageNode.url = `${options.cdnUrl}/${keys[0]}`
          if (!imageNode.data) {
            imageNode.data = {}
          }
          if (!imageNode.data.hProperties) {
            imageNode.data.hProperties = {}
          }
          keys.forEach((key, index) => {
            if (index > 0) {
              if (/\d(x\d)?$/.test(key)) {
                const [width, height] = key.split('x')
                imageNode.data.hProperties.style = `width: ${width}px;`
                if (height) {
                  imageNode.data.hProperties.style = `${imageNode.data.hProperties.style}height: ${height}px`
                }
              } else {
                imageNode.title = key
                imageNode.alt = key
              }
            }
          })
          const [startString, endString] = string.split(wikiLink)
          const newNode = []
          if (startString) {
            newNode.push({
              type: 'text',
              value: startString,
            })
          }
          newNode.push(imageNode)
          if (endString) {
            newNode.push({
              type: 'text',
              value: endString,
            })
          }
          node.children = newNode
        }
      }
    })
  }
}