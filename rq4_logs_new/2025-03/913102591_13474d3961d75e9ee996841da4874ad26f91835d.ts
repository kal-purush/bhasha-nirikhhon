import { adminDB } from "@/app/firebase/server";
import { TH_DATA } from "./constants/coc";
import { THData } from "./types/coc";

const COLLECTION_NAME = "th_levels";
const data: THData = TH_DATA;


async function uploadDataInBatch() {
  // 一度に処理できるドキュメント数には制限があるため、バッチに分けて処理
  const MAX_BATCH_SIZE = 500; // Firestoreの最大バッチサイズは500
  const batches = [];
  
  // データを適切なサイズのバッチに分割
  for (let i = 0; i < data.length; i += MAX_BATCH_SIZE) {
    const batch = adminDB.batch();
    const chunk = data.slice(i, i + MAX_BATCH_SIZE);
    
    chunk.forEach((item) => {
      const docRef = adminDB.collection(COLLECTION_NAME).doc(item.THLevel);
      // THレベルを除外してドキュメントデータを作成
      const { THLevel, ...data } = item;
      batch.set(docRef, data);
    });
    
    batches.push(batch.commit());
  }
  
  try {
    // すべてのバッチをコミット
    await Promise.all(batches);
    console.log(`${data.length}件のデータを${COLLECTION_NAME}コレクションに登録しました`);
  } catch (error) {
    console.error('データ登録中にエラーが発生しました:', error);
  }
}

// データ登録を実行
uploadDataInBatch().then(() => {
  console.log('処理が完了しました');
  process.exit(0);
}).catch(error => {
  console.error('エラーが発生しました:', error);
  process.exit(1);
});