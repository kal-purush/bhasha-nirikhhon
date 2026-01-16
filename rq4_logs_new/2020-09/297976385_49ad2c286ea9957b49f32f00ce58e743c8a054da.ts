import { Controller, Get } from "@overnightjs/core";
import { Request, Response } from "express";

@Controller('transpoco')
export class TranspocoController {

  @Get('')
  public getTranspocoForgeLoggedUser(_: Request, res: Response): void {
    res.send([{
            time: "2020-07-02 08:51:18",
            tracking: [
              {
                uid: 234827,
                tracker_uid: 1101,
                angle: 174,
                speed: 87,
                aquisition_time: 928364,
                visible_satellites: 8,
                engine: "on",
                event_id: 8,
                event_info: 0,
                insert_time: "2020-07-01 08:51:18",
                mileage: "248192.457",
                voltage: 14.3,
                driver_ibutton: 0,
                hdop: 0.8,
              },
            ],
          },
          {
            time: "2020-07-02 08:51:18",
            tracking: [
              {
                uid: 3458765,
                tracker_uid: 1011,
                angle: 174,
                speed: 87,
                aquisition_time: 928364,
                visible_satellites: 8,
                engine: "on",
                event_id: 8,
                event_info: 0,
                insert_time: "2020-07-01 08:51:18",
                mileage: "248192.457",
                voltage: 14.3,
                driver_ibutton: 0,
                hdop: 0.8,
              },
            ],
          }
    ]);
  }
}