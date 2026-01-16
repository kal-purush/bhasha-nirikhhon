import util = require('util');

export default class Column {
  name: string;
  definition: string;
  value: string;

  constructor(name: string | any, value?: string) {
    if (typeof (name) === 'string') {
      this.name = name;
    }
    else {
      Object.keys(name).forEach((key) => {
        this[key] = name[key];
      });
    }
    if (value) {
      this.value = value;
    }
  }

  static parseList(columns:Column[]|any): Column[] {
    if (util.isArray(columns)) {
      return columns;
    }

    let result: Column[] = [];
    Object.keys(columns).forEach((name) => {
      let data = columns[name];
      let args;
      if (typeof (data) === 'string' ||
        typeof (data) === 'number' ||
        data === null ||
        util.isDate(data)) {
        args = {
          name: name,
          value: data
        }
      }
      else {
        args = data;
        args.name = name;
      }
      result.push(new Column(args));
    });
    return result;
  }
}