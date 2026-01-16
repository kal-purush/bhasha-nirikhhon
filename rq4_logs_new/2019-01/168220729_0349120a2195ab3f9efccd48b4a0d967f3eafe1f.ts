import * as Table from "cli-table2";

class Render {
  constructor(private data: any) {}

  hasNoData() {
    if (!this.data && !(this.data instanceof Object)) return false;
    if (this.data.hits.length > 0) return false;
    console.log();
    console.log("No results to display");
    console.log();
    return true;
  }

  display() {
    if (this.hasNoData()) return;

    const results = this.data.hits;
    const head = ["(index)", ...Object.keys(results[0]._source)];
    const table = new Table({ head });

    results.every((result: any, index: number) =>
      table.push([Number(index) + 1, ...Object.values(result._source)])
    );

    console.log(table.toString());
    console.log();
    console.log("TOTAL RESULTS FOUND: ", this.data.total);
    console.log();
  }
}

export default Render;