import { SourceNodesArgs } from 'gatsby'
import { googleFormsToJson } from 'react-google-forms-hooks'

const forms = [
  'https://docs.google.com/forms/d/e/1FAIpQLScmsnAf74APlscnBTfPcvD7NoOmQ5E15ouicJaN72E5D3jbhw/viewform?usp=sf_link',
]

export const sourceNodes = async ({
  actions,
  createNodeId,
  createContentDigest,
}: SourceNodesArgs) => {
  const { createNode } = actions

  forms.map(async item => {
    const form = await googleFormsToJson(item)

    const formNode = {
      form,
      id: createNodeId(form.action),
      parent: null,
      children: [],
      internal: {
        type: 'GoogleForms',
        content: JSON.stringify(form),
        contentDigest: createContentDigest(form),
      },
    }
    createNode(formNode)
  })
}