import {DataObject} from './DataObject';

export class TwoDDataObject {
  get title(): string {
    return this._title;
  }

  set title(value: string) {
    this._title = value;
  }

  get dataObjects(): DataObject[] {
    return this._dataObjects;
  }

  set dataObjects(value: DataObject[]) {
    this._dataObjects = value;
  }

  private _title: string;
  private _dataObjects: DataObject[] = [];

  constructor(title: string, objects: DataObject[]) {
    this._title = title;
    this._dataObjects = objects;
  }
}