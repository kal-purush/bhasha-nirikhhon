import { Configuration } from "./configuration";
import { Dashboard, DashboardType } from "./dashboard";

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