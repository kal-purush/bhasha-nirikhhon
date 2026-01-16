import { Response } from 'express'
import asyncMiddleware from 'middleware-async'
import { construct } from '../../sparql'
import { api } from '../../namespaces'
import { getClient } from '../../read-model/sparqlClient'

export const get = asyncMiddleware(async (req, res: Response) => {
  const graph = await construct()
    .graph(`
      <> a api:Entrypoint ;
        api:dataSheetCollection <data-sheets> .
  
      <data-sheets> a api:DataSheetCollection .`)
    .prefixes({
      api,
    })
    .execute(getClient())

  res.graph(graph)
})