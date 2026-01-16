import { atom } from "recoil";

import { Room } from "../types/room";

export const roomDetailsState = atom<Room>({
  key: "roomDetailsState",
  default: {
    roomId: "fd2aDsAf", // string 部屋ID
    roomName: "成人式2次会", // string 部屋の名前
    memberAmount: 15, // number その部屋にいる参加者の総数
    summary: "場所：ガブリチキン名古屋駅前店", // string 部屋の概要
    isOpen: true, // boolean 参加者受付中かどうか

    lastUpdate: "2022-05-09T00:08:06+09:00", //string 最終更新日 ISO形式
    roomChildren: ["fdahae", "fdjaje"], // Array<string> | null 小部屋のid配列
    roomParent: null, // number? 親部屋のid 親がなければnull
    members: [
      {
        memberId: "f282j10", // string ユーザーId
        name: "たなか", // string ユーザーの名前
        registrationDate: "2022-05-09T00:08:06+09:00", // 登録した時の時間
        comment: "3000円以内のお店がいいです", // string 備考欄（使わないかも）
      },
      {
        memberId: "fen23jw", // string ユーザーId
        name: "すずき",
        registrationDate: "2022-05-09T00:08:06+09:00", // 登録した時の時間
        comment: "",
      },
    ],
    roomMaker: "たなか", // string 部屋を作った人の名前
  },
});