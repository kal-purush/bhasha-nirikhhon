type Record = any;

function getAllFields(records: Record[]) {
  let keyCount: {[key: string]: number} = {};
  // Find all the keys and how many records have it.
  for (let i = 0; i < records.length; ++i) {
    let record = records[i];
    for (let key in record) {
      if (record[key] == null) {
        continue;
      }
      if (!(key in keyCount)) {
        keyCount[key] = 0;
      }
      keyCount[key]++;
    }
  }
  console.log(keyCount);
}

function analyze(records: Record[]) {
  getAllFields(records);
}

function parseLineSeparatedJson(text: string): Record[] {
  let rows = text.split("\n");
  let dataPoints: Record[] = [];
  for (let i = 0; i < rows.length; ++i) {
    if (rows[i] == "") {
      continue;
    }
    try {
      dataPoints.push(JSON.parse(rows[i]));
    } catch (err) {
      console.warn("Failed parsing", rows[i], "into JSON");
      ; // continue
    }
  }
  return dataPoints;
}

d3.text("world_bank.json", function(error, text) {
  let records = parseLineSeparatedJson(text);
  analyze(records);
});