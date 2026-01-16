import { Injectable } from '@angular/core';

@Injectable()
export class UserService {
  user: User;

  constructor() { }

  getUserData() {
    //if authenticated
    //Get user from server
    //Store in user variable

    //Test - Last parameter is null, so a configuration will be generated
    this.user = new User("Jeppe", "Ærenlund", "IT & Digitalisering", "Praktikant");
  }

  //activewidgets is the ID array og widgets
  saveActiveWidgetList(activeWidgets: number[], dashboardId: number) {
    for (var index = 0; index < this.user.configuration.dashboards.length; index++) {
      if(this.user.configuration.dashboards[index].id == dashboardId){
        this.user.configuration.dashboards[index].widgets = activeWidgets;
        break;
      }   
    }
  }
}

export class User {
  first_name: string;
  last_name: string;
  department: string;
  occupation: string;
  configuration: Configuration;

  constructor(
    firstname: string, lastname: string,
    department: string, occupation: string,
    configuration: Configuration = null) {
    this.first_name = firstname; this.last_name = lastname;
    this.department = department; this.occupation = occupation;
    if (configuration != null) {
      this.configuration = configuration;
    }
    else {
      this.configuration = new Configuration()
      this.configuration.dashboards.push(new Dashboard(1, "Nyt dashboard", DashboardType.Standard1Col));
    }
  }
}

export class Configuration {
  dashboards: Dashboard[];

  constructor(dashboards: Dashboard[] = null) {
    if(this.dashboards == null){
      this.dashboards = [];
    }
    else{
      this.dashboards = dashboards;
    }   
  }
}

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