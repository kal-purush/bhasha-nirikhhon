import { atom } from "recoil";

import { Room } from "../types/room";

export const roomState = atom<Room>({
  key: "roomState",
  default: {
    roomId: 1, // string 部屋ID
    roomName: "成人式2次会", // string 部屋の名前
    memberAmount: 15, // number その部屋にいる参加者の総数
    summary: "カラオケ：ここにある", // string 部屋の概要
    isOpen: true, // boolean 参加者受付中かどうか
    lastUpdate: "2022-05-09T00:08:06+09:00", //string 最終更新日 ISO形式
    members: [
      {
        memberId: 1, // string ユーザーId
        name: "たなか", // string ユーザーの名前
        comment: "3000円以内のお店がいいです", // string 備考欄（使わないかも）
        tag: "カラオケ",
      },
      {
        memberId: 2, // string ユーザーId
        name: "すずき",
        comment: "",
        tag: "居酒屋",
      },
      {
        memberId: 3, // string ユーザーId
        name: "やまだ", // string ユーザーの名前
        comment: "3000円以内のお店がいいです", // string 備考欄（使わないかも）
        tag: "ボーリング",
      },
      {
        memberId: 4, // string ユーザーId
        name: "みずたに",
        comment: "",
        tag: "カラオケ",
      },
      {
        memberId: 5, // string ユーザーId
        name: "すぎやま", // string ユーザーの名前
        comment: "3000円以内のお店がいいです", // string 備考欄（使わないかも）
        tag: "カラオケ",
      },
      {
        memberId: 6, // string ユーザーId
        name: "はっとり",
        comment: "",
        tag: "居酒屋",
      },
      {
        memberId: 7, // string ユーザーId
        name: "さとう", // string ユーザーの名前
        comment: "3000円以内のお店がいいです", // string 備考欄（使わないかも）
        tag: "カラオケ",
      },
      {
        memberId: 8, // string ユーザーId
        name: "なるせ",
        comment: "",
        tag: "ボーリング",
      },
      {
        memberId: 9, // string ユーザーId
        name: "みつや", // string ユーザーの名前
        comment: "3000円以内のお店がいいです", // string 備考欄（使わないかも）
        tag: "居酒屋",
      },
      {
        memberId: 10, // string ユーザーId
        name: "さかもと",
        comment: "吐きそう",
        tag: "カラオケ",
      },
    ],
    roomMaker: "たなか", // string 部屋を作った人の名前}
  }
});