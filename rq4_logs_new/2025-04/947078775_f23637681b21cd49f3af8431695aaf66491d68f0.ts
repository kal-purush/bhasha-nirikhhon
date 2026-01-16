import { getDB } from "./getDB";
import { updateDB } from "./updateDB";
import { logUnknownData } from "./logData";
import { transChatGPT } from "./transChatGPT";
import { TransSong } from "./types";
import { sleep } from "openai/core";

const data = await getDB();

// 만약 null로 반환된다면 해당 id와 함께 배열에 담가두다가 끝났을 때 error.txt에 저장

const unknownData: TransSong[] = [];

const transData: TransSong[] = [];

let count = 0;

// for (const song of data) {
//   console.log("count : ", count++);
//   await sleep(150); // 0.15초(150ms) 대기

//   const newSong: TransSong = { ...song };

//   if (song.isTitleJp) {
//     const titleTrans = await transChatGPT(song.title);
//     if (titleTrans === "UNKNOWN" || titleTrans === null) {
//       unknownData.push({ ...newSong, type: "title" });
//     } else {
//       newSong.title = titleTrans;
//     }
//   }
//   if (song.isArtistJp) {
//     const artistTrans = await transChatGPT(song.artist);
//     if (artistTrans === "UNKNOWN" || artistTrans === null) {
//       unknownData.push({ ...song, type: "artist" });
//     } else {
//       newSong.artist = artistTrans;
//     }
//   }
//   if (newSong.isTitleJp || newSong.isArtistJp) {
//     transData.push(newSong);
//   }
// }

// console.log("data : ", data);
// console.log("transData : ", transData);
// console.log("unknownData : ", unknownData);

// let transCount = 0;
// for (const song of transData) {
//   console.log("transCount : ", transCount++);
//   if (song) {
//     updateDB(song);
//   }
// }

// 만약 unknownData가 있다면 해당 데이터를 배열에 담아서 끝났을 때 error.txt에 저장
if (unknownData.length > 0) {
  logUnknownData(unknownData, "errorLog.txt");
}

if (transData.length > 0) {
  logUnknownData(transData, "transDataLog.txt");
}