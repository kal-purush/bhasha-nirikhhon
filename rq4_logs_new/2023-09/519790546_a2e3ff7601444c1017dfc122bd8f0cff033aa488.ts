import dayjs from "dayjs";
import {
  AvailablePlatformType,
  EventGenreType,
  VrcEventCalenderType,
} from "../../types/VrcEventCalenderType";

const originalUrl =
  "https://docs.google.com/forms/d/e/1FAIpQLSevo0ax6ALIzllRCT7up-3KZkohD3VfG28rcOy8XMqDwRWevQ/viewform?usp=pp_url";

const googleFormEntryIds = {
  eventName: "426573786",
  availablePlatform: "1261006949",
  date: "450203369",
  startTime: "1010494053",
  endTime: "203043324",
  eventOwner: "1540217995",
  eventContent: "701384676",
  eventGenre: "1606730788",
  participationConditions: "2064647146",
  wayToParticipate: "1285455202",
  note: "586354013",
};

// TODO: 名前を変える。AvailablePlatformを受け取って、文字列を返す関数なのでreplaceAvailablePlatformForUrl
const getAvailablePlatform = (availablePlatform: AvailablePlatformType) => {
  switch (availablePlatform) {
    case "PC&Quest":
      return "PC/Quest両対応（Quest対応）";
      break;
    case "PC":
      return "PC対応";
      break;
    case "Quest":
      return "Quest対応";
      break;
  }
};

const getEventGenreArray = (eventGenres: EventGenreType) =>
  Object.entries(eventGenres)
    .filter((genre) => genre[1] === true)
    .map((genre) => {
      {
        switch (genre[0]) {
          case "avatarFittingEvent":
            return "アバター試着会";
            break;
          case "modifiedAvaterExchangeEvent":
            return "改変アバター交流会";
            break;
          case "otherNetworkingEvent":
            return "その他交流会";
            break;
          case "vrDrinkingEvent":
            return "VR飲み会";
            break;
          case "storeEvent":
            return "店舗系イベント";
            break;
          case "musicEvent":
            return "音楽系イベント";
            break;
          case "academicEvent":
            return "学術系イベント";
            break;
          case "rolePlayEvent":
            return "ロールプレイ";
            break;
          case "forBeginnersEvent":
            return "初心者向けイベント";
            break;
          case "regularEvent":
            return "定期イベント";
            break;
          default:
            return "";
            break;
        }
      }
    });

export const createVrcEventCalenderUrl = (values: VrcEventCalenderType) => {
  const eventCalenderUrl = [
    originalUrl,
    `&entry.${googleFormEntryIds.eventName}=${values.eventName}`,
    `&entry.${googleFormEntryIds.availablePlatform}=${getAvailablePlatform(
      values.availablePlatform
    )}`,
    `&entry.${googleFormEntryIds.date}=${dayjs(values.date).format(
      "YYYY-MM-DD"
    )}`,
    `&entry.${googleFormEntryIds.startTime}=${values.startTime}`,
    `&entry.${googleFormEntryIds.endTime}=${values.endTime}`,
    `&entry.${googleFormEntryIds.eventOwner}=${values.eventOwner}`,
    `&entry.${googleFormEntryIds.eventContent}=${values.eventContent}`,
    `&entry.${googleFormEntryIds.participationConditions}=${values.participationConditions}`,
    `&entry.${googleFormEntryIds.wayToParticipate}=${values.wayToParticipate}`,
    `&entry.${googleFormEntryIds.note}=${values.note}`,
  ];
  const eventGenreUrl: string[] = [];
  getEventGenreArray(values.eventGenre).forEach((genre) =>
    eventCalenderUrl.push(`&entry.${googleFormEntryIds.eventGenre}=${genre}`)
  );
  eventCalenderUrl.push(eventGenreUrl.join(""));
  console.dir(eventCalenderUrl.join(""));
  return eventCalenderUrl.join("");
};