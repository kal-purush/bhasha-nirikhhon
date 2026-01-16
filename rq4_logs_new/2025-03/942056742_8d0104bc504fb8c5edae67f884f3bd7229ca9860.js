/**
 * Bonding Curve Model for Memecoin Launchpad
 * 
 * This module implements the core mathematics and logic for the bonding curve
 * pricing model used in the platform. The bonding curve ensures that token
 * prices adjust dynamically based on supply and demand, creating a fair and
 * transparent market.
 */

const BigNumber = require('bignumber.js');

// Set precision for calculations
BigNumber.config({ DECIMAL_PLACES: 18, ROUNDING_MODE: BigNumber.ROUND_DOWN });

class BondingCurveModel {
  constructor(params = {}) {
    // Default parameters
    this.initialSupply = new BigNumber(params.initialSupply || '800000000'); // 800 million tokens
    this.reserveRatio = new BigNumber(params.reserveRatio || '0.2'); // 20% reserve ratio
    this.initialPrice = new BigNumber(params.initialPrice || '0.0000001'); // Initial price in base currency
    this.fee = new BigNumber(params.fee || '0.01'); // 1% fee
    this.currentSupply = new BigNumber(params.currentSupply || this.initialSupply);
    this.poolBalance = this.calculateInitialPoolBalance();
    
    // Liquidity milestones (in base currency)
    this.liquidityMilestones = {
      addDexLiquidity: new BigNumber('69000'),
      firstBurn: new BigNumber('100000'),
      secondBurn: new BigNumber('1000000')
    };
    
    // Burn percentages at milestones
    this.burnPercentages = {
      firstBurn: new BigNumber('0.1'), // 10%
      secondBurn: new BigNumber('0.05') // 5%
    };
  }

  /**
   * Calculate the initial pool balance based on the bonding curve formula
   * @returns {BigNumber} The initial pool balance
   */
  calculateInitialPoolBalance() {
    return this.initialSupply
      .times(this.initialPrice)
      .times(this.reserveRatio);
  }

  /**
   * Calculate the current token price based on the current supply
   * @returns {BigNumber} The current token price
   */
  getCurrentPrice() {
    return this.poolBalance
      .div(this.currentSupply.times(this.reserveRatio));
  }

  /**
   * Calculate how many tokens can be purchased with a given amount of base currency
   * @param {BigNumber|string|number} baseAmount - Amount of base currency to spend
   * @returns {Object} Object containing tokens received and fee amount
   */
  calculateBuyTokens(baseAmount) {
    const baseAmountBN = new BigNumber(baseAmount);
    
    // Calculate fee
    const feeAmount = baseAmountBN.times(this.fee);
    const baseAmountAfterFee = baseAmountBN.minus(feeAmount);
    
    // Calculate tokens to receive
    const newPoolBalance = this.poolBalance.plus(baseAmountAfterFee);
    const newSupply = this.calculatenewSupply(newPoolBalance);
    const tokensToReceive = newSupply.minus(this.currentSupply);
    
    return {
      tokensReceived: tokensToReceive,
      fee: feeAmount,
      newPrice: this.poolBalance
        .div(this.currentSupply.times(this.reserveRatio))
    };
  }
  
  /**
   * Calculate new supply based on new pool balance
   * @param {BigNumber} newPoolBalance - New pool balance after purchase
   * @returns {BigNumber} New token supply
   */
  calculatenewSupply(newPoolBalance) {
    return this.currentSupply.times(
      newPoolBalance.div(this.poolBalance).pow(new BigNumber(1).div(this.reserveRatio))
    );
  }

  /**
   * Calculate how much base currency will be received for selling a given amount of tokens
   * @param {BigNumber|string|number} tokenAmount - Amount of tokens to sell
   * @returns {Object} Object containing base currency received and fee amount
   */
  calculateSellTokens(tokenAmount) {
    const tokenAmountBN = new BigNumber(tokenAmount);
    
    // Ensure user has enough tokens
    if (tokenAmountBN.gt(this.currentSupply)) {
      throw new Error('Insufficient token balance');
    }
    
    // Calculate new supply and pool balance after sale
    const newSupply = this.currentSupply.minus(tokenAmountBN);
    const newPoolBalance = this.calculatePoolBalance(newSupply);
    const baseAmountBeforeFee = this.poolBalance.minus(newPoolBalance);
    
    // Calculate fee
    const feeAmount = baseAmountBeforeFee.times(this.fee);
    const baseAmountAfterFee = baseAmountBeforeFee.minus(feeAmount);
    
    return {
      baseReceived: baseAmountAfterFee,
      fee: feeAmount,
      newPrice: newPoolBalance.div(newSupply.times(this.reserveRatio))
    };
  }
  
  /**
   * Calculate pool balance for a given supply
   * @param {BigNumber} supply - Token supply
   * @returns {BigNumber} Pool balance
   */
  calculatePoolBalance(supply) {
    return this.poolBalance.times(
      supply.div(this.currentSupply).pow(this.reserveRatio)
    );
  }

  /**
   * Execute a token purchase
   * @param {BigNumber|string|number} baseAmount - Amount of base currency to spend
   * @returns {Object} Transaction details
   */
  executeBuy(baseAmount) {
    const result = this.calculateBuyTokens(baseAmount);
    
    // Update state
    this.poolBalance = this.poolBalance.plus(
      new BigNumber(baseAmount).minus(result.fee)
    );
    this.currentSupply = this.currentSupply.plus(result.tokensReceived);
    
    // Check if we've reached any milestones
    this.checkMilestones();
    
    return {
      tokensReceived: result.tokensReceived,
      fee: result.fee,
      newPrice: this.getCurrentPrice(),
      currentSupply: this.currentSupply,
      poolBalance: this.poolBalance
    };
  }

  /**
   * Execute a token sale
   * @param {BigNumber|string|number} tokenAmount - Amount of tokens to sell
   * @returns {Object} Transaction details
   */
  executeSell(tokenAmount) {
    const result = this.calculateSellTokens(tokenAmount);
    
    // Update state
    this.currentSupply = this.currentSupply.minus(new BigNumber(tokenAmount));
    this.poolBalance = this.poolBalance.minus(
      result.baseReceived.plus(result.fee)
    );
    
    return {
      baseReceived: result.baseReceived,
      fee: result.fee,
      newPrice: this.getCurrentPrice(),
      currentSupply: this.currentSupply,
      poolBalance: this.poolBalance
    };
  }

  /**
   * Check if any milestones have been reached and perform necessary actions
   */
  checkMilestones() {
    const marketCap = this.currentSupply.times(this.getCurrentPrice());
    
    // Add DEX liquidity at $69,000 market cap
    if (marketCap.gte(this.liquidityMilestones.addDexLiquidity)) {
      // Implementation for adding liquidity to DEX would go here
      // This would typically involve integrating with a DEX like Raydium or Uniswap
    }
    
    // Burn 10% of liquidity at $100,000 market cap
    if (marketCap.gte(this.liquidityMilestones.firstBurn)) {
      this.burnLiquidity(this.burnPercentages.firstBurn);
    }
    
    // Burn additional 5% of liquidity at $1,000,000 market cap
    if (marketCap.gte(this.liquidityMilestones.secondBurn)) {
      this.burnLiquidity(this.burnPercentages.secondBurn);
    }
  }

  /**
   * Burn a percentage of the liquidity pool
   * @param {BigNumber} percentage - Percentage to burn
   */
  burnLiquidity(percentage) {
    const burnAmount = this.poolBalance.times(percentage);
    this.poolBalance = this.poolBalance.minus(burnAmount);
    
    // In a real implementation, this would interact with the blockchain
    // to actually burn the tokens
    return {
      burnAmount,
      newPoolBalance: this.poolBalance
    };
  }

  /**
   * Get current market cap
   * @returns {BigNumber} Current market cap
   */
  getMarketCap() {
    return this.currentSupply.times(this.getCurrentPrice());
  }

  /**
   * Get current token stats
   * @returns {Object} Token statistics
   */
  getTokenStats() {
    const currentPrice = this.getCurrentPrice();
    return {
      currentSupply: this.currentSupply.toString(),
      poolBalance: this.poolBalance.toString(),
      currentPrice: currentPrice.toString(),
      marketCap: this.currentSupply.times(currentPrice).toString(),
      reserveRatio: this.reserveRatio.toString()
    };
  }
}

module.exports = BondingCurveModel;