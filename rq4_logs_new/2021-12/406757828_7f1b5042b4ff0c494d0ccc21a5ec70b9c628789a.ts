import { ItemColor } from '@src/generated/graphql';

export const allColors = [
  ItemColor.Black,
  ItemColor.White,
  ItemColor.Brown,
  ItemColor.Gray,
  ItemColor.Khaki,
  ItemColor.Green,
  ItemColor.Blue,
  ItemColor.Navy,
  ItemColor.Purple,
  ItemColor.Pink,
  ItemColor.Red,
  ItemColor.WineRed,
  ItemColor.Yellow,
  ItemColor.Orange,
  ItemColor.Beige,
  ItemColor.Transparent,
  ItemColor.Gold,
  ItemColor.Silver,
];

export function colorText(color: ItemColor) {
  switch (color) {
    case ItemColor.Beige:
      return 'ベージュ';
    case ItemColor.Black:
      return 'ブラック';
    case ItemColor.Blue:
      return 'ブルー';
    case ItemColor.Brown:
      return 'ブラウン';
    case ItemColor.Gray:
      return 'グレイ';
    case ItemColor.Green:
      return 'グリーン';
    case ItemColor.Khaki:
      return 'カーキ';
    case ItemColor.Navy:
      return 'ネイビー';
    case ItemColor.Orange:
      return 'オレンジ';
    case ItemColor.Pink:
      return 'ピンク';
    case ItemColor.Purple:
      return 'パープル';
    case ItemColor.Red:
      return 'レッド';
    case ItemColor.Gold:
      return 'ゴールド';
    case ItemColor.Silver:
      return 'シルバー';
    case ItemColor.Transparent:
      return '透明';
    case ItemColor.White:
      return 'ホワイト';
    case ItemColor.WineRed:
      return 'ワインレッド';
    case ItemColor.Yellow:
      return 'イエロー';
  }
}

export function bgColorCSS(color: ItemColor) {
  switch (color) {
    case ItemColor.Beige:
      return 'bg-orange-200';
    case ItemColor.Black:
      return 'bg-black';
    case ItemColor.Blue:
      return 'bg-blue-500';
    case ItemColor.Brown:
      return 'bg-brown-500';
    case ItemColor.Gold:
      return 'bg-yellow-500';
    case ItemColor.Gray:
      return 'bg-gray-500';
    case ItemColor.Green:
      return 'bg-green-500';
    case ItemColor.Khaki:
      return 'bg-lime-800';
    case ItemColor.Navy:
      return 'bg-blue-800';
    case ItemColor.Orange:
      return 'bg-orange-500';
    case ItemColor.Pink:
      return 'bg-pink-500';
    case ItemColor.Purple:
      return 'bg-purple-500';
    case ItemColor.Red:
      return 'bg-red-600';
    case ItemColor.Silver:
      return 'bg-slate-400';
    case ItemColor.Transparent:
      return '';
    case ItemColor.White:
      return 'bg-white';
    case ItemColor.WineRed:
      return 'bg-rose-700';
    case ItemColor.Yellow:
      return 'bg-yellow-400';
  }
}