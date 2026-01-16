const extractNumericValueFromPrice = (price: string): number => {
  const splitPrice = price.split(" ");

  const separatedValue = splitPrice.slice(-1)[0].split(",");

  const joinedNumber = separatedValue.join("");

  return parseFloat(joinedNumber);
};

export const verifyCartTotal = (
  itemTax: string,
  itemDiscount: string,
  itemQuantity: string,
  itemRawPrice: string,
  itemMoney: string
) => {
  let itemTaxInt = extractNumericValueFromPrice(itemTax);
  let itemDiscountInt = extractNumericValueFromPrice(itemDiscount);
  let itemQuantityInt = extractNumericValueFromPrice(itemQuantity);
  let itemRawPriceInt = extractNumericValueFromPrice(itemRawPrice);
  let itemTotalMoneyInt = extractNumericValueFromPrice(itemMoney);

  const itemTotalPriceMatchesParts =
    itemTotalMoneyInt ===
    itemDiscountInt + itemTaxInt + itemQuantityInt * itemRawPriceInt;

  if (itemTotalPriceMatchesParts) {
    return true;
  }
  return false;
};