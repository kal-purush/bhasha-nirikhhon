export class Dashboard {
  public id: number;
  name: string;
  type: DashboardType;
  widgets: number[]; //IDs of widgets in order

  constructor(id: number, name: string, type: DashboardType, widgets: number[] = null) {
    this.id = id;
    this.name = name;
    this.type = type;
    if (this.widgets == null) {
      this.widgets = [];
    }
    else {
      this.widgets = widgets;
    }
  }
}

export enum DashboardType {
  Standard1Col = 1,
  Standard2Col = 2,
  TopWidgets2Col = 3,
  TopWidgets1Col = 4
}