export class MoneyBack{
  /**
   * Calculate the money that the user will get back 
   * 
   * @param {number} typeBet_lose The cancel rate of the type bet
   * @param {number} typePay_lose The cancel rate of the type pay
   * @param {number} coins The coins that the user did bet
   * 
   * @return {number} The coins that the user will get back
   */
  static getMoneyBack(typeBet_lose:number, typePay_lose:number, coins:number){
    if(typePay_lose == 100) return 0;
    return coins-Math.round(coins*(typeBet_lose+typePay_lose));
  }
}