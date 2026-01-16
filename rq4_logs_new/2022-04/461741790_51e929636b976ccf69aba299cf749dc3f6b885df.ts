import { TTimetableDocument, TLineDocument, TStationDocument } from "./DBCtrler.types"

export const DEFAULT_DATE = new Date(2020, 0, 1, 0, 0, 0, 0);

export const getTodaysDate = () => new Date(new Date(Date.now()).setHours(0, 0, 0, 0));

export const LineDocInitValue: TLineDocument = {
  can_read: [],
  can_write: [],
  disp_name: "",
  tag_list: [],
  hashed_read_pw: new Map<string, Date>(),
  hashed_write_pw: new Map<string, Date>(),
  time_multipl: 1,
};

export const TimetableDocInitValue: TTimetableDocument = {
  tags: [],
  train_id: "",
  sec_sys_train_id: "",
  sec_sys_sta_pass_setting: false,
  direction: "Inbound",
  radio_ch: "",
  line_color: "",
  train_type: "",
  dep_from_name: "",
  dep_from_time: DEFAULT_DATE,
  dep_from_track_num: "",
  work_to_name: "",
  work_to_time: DEFAULT_DATE,
  work_to_track_num: "",
  last_stop_name: "",
  last_stop_time: DEFAULT_DATE,
  last_stop_track_num: "",
  office_name: "",
  work_number: "",
  effected_date: getTodaysDate(),
  additional_info: "",
  next_work: null,
};

export const StationDocInitValue: TStationDocument = {
  full_name: "",
  name_len_4: "",
  location: 0.0,
  required_time_to_this_sta: 0,
  arrive_time: DEFAULT_DATE,
  departure_time: DEFAULT_DATE,
  is_pass: false,
  arr_symbol: "",
  dep_symbol: "",
  track_num: "",
  run_in_limit: 0,
  run_out_limit: 0,
  sta_work: "",
  sta_color: ""
};