import {date, number, object, preprocess, string} from "zod";

export const bookingSchema = object({
  room_id: number({required_error: "Room ID is required "}).min(1, "Room ID should be greater than zero"),
  check_in: date({required_error: "Check in date is required"}),
  duration_id: number({required_error: "Duration ID is required"}).min(1, "Duration ID should be greater than zero"),
  status_id: number({required_error: "Status ID is required"}).min(1, "Status ID should be greater than zero"),
  tenant_id: string({required_error: "Tenant ID is required"}).cuid("Invalid Tenant ID"),
  fee: preprocess(
    (val) => {
      if (val == undefined) {
        return undefined;
      }

      if (typeof val == "string") {
        return Number(val);
      }

      return val;
    },
    number({required_error: "Fee is required"})
      .min(1, "Fee should be greater than 0")
  ),
});