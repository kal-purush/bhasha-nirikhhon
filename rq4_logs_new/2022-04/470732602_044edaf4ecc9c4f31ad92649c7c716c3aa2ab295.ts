import { joinPathFragments, Tree } from '@nrwl/devkit';
import { OpenAPIV3 as OpenAPI } from 'openapi-types';
import { ScraperSchema } from './schema.type';

export function writeSwaggerJson(
  tree: Tree,
  doc: OpenAPI.Document,
  schema: ScraperSchema
) {
  // write each component schema to individual files
  Object.entries(doc.components.schemas).forEach(([modelName, modelValue]) => {
    tree.write(
      joinPathFragments(schema.dir, getModelPath(modelName)),
      JSON.stringify(modelValue)
    );
  });

  // given a standalone swagger document,
  // split all the #/components/schemas
  // into separate files and $refs
  tree.write(
    joinPathFragments(schema.dir, 'openapi.json'),
    JSON.stringify(replaceComponentsWith$RefPaths(doc))
  );
}

function replaceComponentsWith$RefPaths(
  doc: OpenAPI.Document
): OpenAPI.Document {
  return {
    ...doc,
    components: {
      schemas: Object.keys(doc.components.schemas).reduce((acc, curr) => {
        return {
          ...acc,
          [curr]: <OpenAPI.ReferenceObject>{
            $ref: getModelPath(curr),
          },
        };
      }, {}),
    },
  };
}

function getModelPath(name: string): string {
  return joinPathFragments('components', `${name}.json`);
}