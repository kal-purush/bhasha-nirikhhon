import { Injectable } from '@angular/core';
import { UserService, Dashboard } from "../user-service/user.service";

@Injectable()
export class DashboardcontrollerService {
  
  public addWidgetEvent: { (widgetId: number): void; }; //events
  public removeWidgetEvent: { (widgetId: number): void; };
  public changeDashboardEvent: {( dashboardId: number): void; };

  private dashboards: Dashboard[];
  private activeDashboard: Dashboard;


  constructor(private userService: UserService) { 
    this.dashboards = [];
    //Setup
    this.getDashboardConfiguration();
    //start with the 1st one as active dashboard
    this.activeDashboard = this.dashboards[0];
  }
  
  //loads in the dashboard configuration from user service
  private getDashboardConfiguration(){
    this.dashboards = this.userService.user.configuration.dashboards;
  }

  //Fire event to subscribers
  changeDashboard(dashboardId: number){
    this.dashboards.forEach(dboard => {
      if(dboard.id == dashboardId){
        this.activeDashboard = dboard;
      }
    });
    this.changeDashboardEvent(dashboardId);

  }

  //Fire event to subscribers
  addWidget(widgetId: number){
    this.addWidgetEvent(widgetId);
  }

  //Fire event to subscribers
  removeWidget(widgetId: number){
    this.removeWidgetEvent(widgetId);
  }

  getDashboards (): Dashboard[]{
    return this.dashboards;
  }

  getActiveDashboard(): Dashboard{
    return this.activeDashboard;
  }

  //Get widgetsIds for specific dashboard
  getWidgets(dashboardId: number){
    this.dashboards.forEach(dashboard => {
      if(dashboard.id == dashboardId){
        return dashboard.widgets;
      }
    });   
  }

}

// Har alle dashboards som objekter
// Har har liste med aktive widgets på hvert dashboard