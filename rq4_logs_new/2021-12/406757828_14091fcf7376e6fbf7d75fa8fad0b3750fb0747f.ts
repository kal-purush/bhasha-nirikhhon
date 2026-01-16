import { ItemSellingPlatform } from '@src/generated/graphql';

export function changeItemImageSize(
  imgUrl: string,
  platform: ItemSellingPlatform,
  size: number
) {
  switch (platform) {
    case ItemSellingPlatform.Rakuten:
      return changeRakutenImageSize(imgUrl, size);
    case ItemSellingPlatform.YahooShopping:
      return changeYahooShoppingImageSize(imgUrl, size);
    case ItemSellingPlatform.PaypayMall:
      return changeYahooShoppingImageSize(imgUrl, size);
    default:
      return imgUrl;
  }
}

function changeRakutenImageSize(rakutenImgUrl: string, size: number): string {
  const imgUrl = new URL(rakutenImgUrl);
  imgUrl.searchParams.set('_ex', `${size}x${size}`);
  return imgUrl.toString();
}

function changeYahooShoppingImageSize(
  yahooImgUrl: string,
  size: number
): string {
  // TODO: Adjust size depending on the given size
  //  Yahoo shopping item image is a (smallest) ~ z (biggest)
  if (size >= 500) {
    console.log(size);
    return yahooImgUrl.replace('/g/', '/n/');
  }
  return yahooImgUrl;
}