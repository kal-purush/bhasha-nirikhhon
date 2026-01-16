import {Bookmark} from './type';
import {get} from './get';

/**
 * ブックマークのタイトル変更
 */
 export const rename = (id: Bookmark['id'],title: Bookmark['title']) => {
    // ブラウザからブックマーク配列を取得
    const bookmarks: Bookmark[] = get();
    // ブックマーク配列から引数のIDと一致するブックマークオブジェクトを取得
    const target: Bookmark | undefined = bookmarks.find(bookmark => bookmark.id === id);
    // 引数のIDと一致するブックマークオブジェクトが存在するか判定
    if (target !== undefined) {
      // 引数のタイトルに変更
      target.title = title
    }
    // ブックマーク配列をブラウザに保存
    window.localStorage.setItem('bookmarks', JSON.stringify(bookmarks))
  }