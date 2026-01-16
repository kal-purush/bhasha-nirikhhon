
import {IRocket} from "./IRocket";
import {ITelemetry} from "./ITelemetry";
import {IReuse} from "./IReuse";
import {ILinks} from "./ILinks";
import {ILaunchSite} from "./ILaunchSite";
/**
 * Created by alexandremenielle on 26/05/2018.
 */

export interface ILaunch {
  flight_number: number;
  mission_name: string;
  launch_year: string;
  launch_date_unix: number;
  launch_date_utc: string;
  launch_date_local: string;
  rocket: IRocket;
  telemetry: ITelemetry;
  reuse: IReuse;
  launch_site: ILaunchSite;
  launch_success?: boolean;
  links: ILinks;
  details?: string;
}