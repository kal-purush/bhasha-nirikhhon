import { Client } from "elasticsearch";
import { string } from "@oclif/parser/lib/flags";

type SearchArgs = {
  index: string;
  query: string;
  client: Client;
  fields?: Array<string>;
  offset?: number;
  size?: number;
  type?: string;
  fuzziness?: any;
};

class Search {
  constructor(private args: SearchArgs) {}
  async hasNoIndex(index: string) {
    const { client } = this.args;
    if (!(await client.indices.exists({ index }))) {
      console.log(`Specified index ${index} does not exist`);
      return true;
    }
    return false;
  }
  parseSearchQuery(query: string) {
    const array = query.split(":");
    const term = array.splice(-1)[0];
    return {
      fields: array.length <= 0 ? undefined : array,
      term
    };
  }
  async find() {
    let { index, query, client, offset, size, fuzziness, type } = this.args;
    if (await this.hasNoIndex(index)) return;
    const { fields, term } = this.parseSearchQuery(query);
    fuzziness = fuzziness ? 2 : undefined;

    try {
      const response = await client.search({
        index,
        body: {
          from: offset,
          type,
          size,
          query: {
            multi_match: {
              query: `${term}`,
              fuzziness,
              operator: "and",
              fields,
              type: "most_fields"
            }
          }
        }
      });

      console.table(response.hits.hits.map(record => record._source));

      // return new Render().results(response.hits);
    } catch (error) {
      console.log(error.message);
      return;
    }
  }
}

export default Search;