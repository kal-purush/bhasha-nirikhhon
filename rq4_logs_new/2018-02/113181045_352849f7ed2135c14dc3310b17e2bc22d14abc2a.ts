import { Op } from "sequelize";
import { Station } from "../../main/models";
import { StationAttributes, StationInstance } from "../../main/models/station";
import { SpecUtil } from "../SpecUtil";

describe("The Station model", (): void => {
   it("can be created and destroyed", function (done: DoneFn): void {
      const stationAttributes: StationAttributes = {
         name: "name",
         latitude: 1.2,
         longtitude: 5.6,
         conductorAt: true,
      };
      let station: StationInstance;

      Station.create(stationAttributes).then((result: StationInstance): Promise<void> => {
         expect(result).toBeTruthy();
         SpecUtil.verifyInstance(result, stationAttributes);
         station = result;
         return result.destroy();
      }).then((): Promise<StationInstance> => {
         return Station.findById((station as any).id);
      }).then((result: StationInstance): void => {
         expect(result).toBeFalsy();
      }).then(done).catch(done.fail);
   });
});