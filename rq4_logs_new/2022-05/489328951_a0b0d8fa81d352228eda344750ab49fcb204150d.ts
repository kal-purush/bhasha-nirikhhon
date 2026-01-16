import { Asset } from '../types/asset';

export const useTotalByBrand = (data: Asset[]) => {
  const choiseAsset = data.map((asset) => {
    const name = asset.name.slice(0, 5);
    const result = asset.numberOfPossessions * asset.marketPrice;
    return { name, result };
  });
  const sort = choiseAsset.sort((a, b) => b.result - a.result);
  const sort2 = sort.slice(0, 9);
  const sort3 = sort.slice(9, sort.length);
  const sort4 = sort3.reduce((sum, element) => sum + element.result, 0);
  const sort5 = [...sort2, { name: 'その他', result: sort4 }];
  const name = sort5.map((n) => n.name);
  const result = sort5.map((r) => r.result);
  return { name, result };
};