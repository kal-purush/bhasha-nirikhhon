import { VrcEventCalenderType } from "../../types/VrcEventCalenderType";
import { createVrcEventCalenderUrl } from "./createVrcEventCalenderUrl";

const mock: VrcEventCalenderType = {
  eventName: "イベント名",
  availablePlatform: "PC&Quest",
  date: "2021-10-10",
  startTime: "22:00",
  endTime: "23:30",
  eventOwner: "イベント主催者",
  eventContent: "イベント内容イベント内容イベント内容イベント内容",
  eventGenre: {
    avatarFittingEvent: false,
    modifiedAvatarExchangeEvent: false,
    otherNetworkingEvent: true,
    vrDrinkingEvent: true,
    storeEvent: false,
    musicEvent: false,
    academicEvent: false,
    rolePlayEvent: false,
    forBeginnersEvent: false,
    regularEvent: true,
  },
  participationConditions: "参加条件参加条件参加条件参加条件",
  wayToParticipate: "参加方法参加方法参加方法参加方法",
  note: "備考備考備考備考",
  noticeForOverseasUsers: true,
};

test("mockで渡した値が正しいURLに変換される", () => {
  const result = createVrcEventCalenderUrl(mock);
  expect(result).toBe(
    "https://docs.google.com/forms/d/e/1FAIpQLSevo0ax6ALIzllRCT7up-3KZkohD3VfG28rcOy8XMqDwRWevQ/viewform?usp=pp_url&entry.426573786=%E3%82%A4%E3%83%99%E3%83%B3%E3%83%88%E5%90%8D&entry.1261006949=PC%2FQuest%E4%B8%A1%E5%AF%BE%E5%BF%9C%EF%BC%88Quest%E5%AF%BE%E5%BF%9C%EF%BC%89&entry.450203369=2021-10-10&entry.1010494053=22%3A00&entry.203043324=23%3A30&entry.1540217995=%E3%82%A4%E3%83%99%E3%83%B3%E3%83%88%E4%B8%BB%E5%82%AC%E8%80%85&entry.701384676=%E3%82%A4%E3%83%99%E3%83%B3%E3%83%88%E5%86%85%E5%AE%B9%E3%82%A4%E3%83%99%E3%83%B3%E3%83%88%E5%86%85%E5%AE%B9%E3%82%A4%E3%83%99%E3%83%B3%E3%83%88%E5%86%85%E5%AE%B9%E3%82%A4%E3%83%99%E3%83%B3%E3%83%88%E5%86%85%E5%AE%B9&entry.1606730788=%E3%81%9D%E3%81%AE%E4%BB%96%E4%BA%A4%E6%B5%81%E4%BC%9A&entry.1606730788=VR%E9%A3%B2%E3%81%BF%E4%BC%9A&entry.1606730788=%E5%AE%9A%E6%9C%9F%E3%82%A4%E3%83%99%E3%83%B3%E3%83%88&entry.2064647146=%E5%8F%82%E5%8A%A0%E6%9D%A1%E4%BB%B6%E5%8F%82%E5%8A%A0%E6%9D%A1%E4%BB%B6%E5%8F%82%E5%8A%A0%E6%9D%A1%E4%BB%B6%E5%8F%82%E5%8A%A0%E6%9D%A1%E4%BB%B6&entry.1285455202=%E5%8F%82%E5%8A%A0%E6%96%B9%E6%B3%95%E5%8F%82%E5%8A%A0%E6%96%B9%E6%B3%95%E5%8F%82%E5%8A%A0%E6%96%B9%E6%B3%95%E5%8F%82%E5%8A%A0%E6%96%B9%E6%B3%95&entry.586354013=%E5%82%99%E8%80%83%E5%82%99%E8%80%83%E5%82%99%E8%80%83%E5%82%99%E8%80%83&entry.1607289186=%E5%B8%8C%E6%9C%9B%E3%81%99%E3%82%8B"
  );
});