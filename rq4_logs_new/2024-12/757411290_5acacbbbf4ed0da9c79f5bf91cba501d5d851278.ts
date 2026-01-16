import { FairholdLandPurchase } from "./tenure/FairholdLandPurchase";
import { FairholdLandRent } from "./tenure/FairholdLandRent";
import { MarketRent } from "./tenure/MarketRent";
import { SocialRent } from "./tenure/SocialRent";

import { Fairhold } from "./Fairhold";
import { DEFAULT_FORECAST_PARAMETERS } from "./ForecastParameters"; // TODO: should this be saved to constants.ts instead?
import { Household } from "./Household";
import { socialRentAdjustmentTypes } from "../data/socialRentAdjustmentsRepo";
import { Lifetime } from "./Lifetime";
import { Mortgage } from "./Mortgage";
import { Property } from "./Property";
import { MarketPurchase } from "./tenure/MarketPurchase";

const socialRentAdjustments: socialRentAdjustmentTypes = [
  {
    inflation: 3.3,
    total: 4.3,
    year: "2001-02",
    additional: 0,
  },
  {
    inflation: 1.7,
    total: 2.2,
    year: "2002-03",
    additional: 0,
  },
  {
    inflation: 1.7,
    total: 2.2,
    year: "2003-04",
    additional: 0,
  },
  {
    inflation: 2.8,
    total: 3.3,
    year: "2004-05",
    additional: 0,
  },
  {
    inflation: 3.1,
    total: 3.6,
    year: "2005-06",
    additional: 0,
  },
  {
    inflation: 2.7,
    total: 3.2,
    year: "2006-07",
    additional: 0,
  },
  {
    inflation: 3.6,
    total: 4.1,
    year: "2007-08",
    additional: 0,
  },
  {
    inflation: 3.9,
    total: 4.4,
    year: "2008-09",
    additional: 0,
  },
  {
    inflation: 5.0,
    total: 5.5,
    year: "2009-10",
    additional: 0,
  },
  {
    inflation: -1.4,
    total: -0.9,
    year: "2010-11",
    additional: 0,
  },
  {
    inflation: 4.6,
    total: 5.1,
    year: "2011-12",
    additional: 0,
  },
  {
    inflation: 5.6,
    total: 6.1,
    year: "2012-13",
    additional: 0,
  },
  {
    inflation: 2.6,
    total: 3.1,
    year: "2013-14",
    additional: 0,
  },
  {
    inflation: 3.2,
    total: 3.7,
    year: "2014-15",
    additional: 0,
  },
  {
    inflation: 1.2,
    total: 2.2,
    year: "2015-16",
    additional: 0,
  },
  {
    inflation: NaN,
    total: -1.0,
    year: "2016-17",
    additional: 0,
  },
  {
    inflation: NaN,
    total: -1.0,
    year: "2017-18",
    additional: 0,
  },
  {
    inflation: NaN,
    total: -1.0,
    year: "2018-19",
    additional: 0,
  },
  {
    inflation: NaN,
    total: -1.0,
    year: "2019-20",
    additional: 0,
  },
  {
    inflation: 1.7,
    total: 2.7,
    year: "2020-21",
    additional: 0,
  },
  {
    inflation: 0.5,
    total: 1.5,
    year: "2021-22",
    additional: 0,
  },
  {
    inflation: 3.1,
    total: 4.1,
    year: "2022-23",
    additional: 0,
  },
  {
    inflation: 10.1,
    total: 11.1,
    year: "2023-24",
    additional: 0,
  },
];

/**
 * Creates a simplified instance of the `Household` class
 * @param overrides Include custom values to overwrite those provided. Default values are:
 * `averageRentYearly: 1200,
 * socialRentAverageEarning: 354.1,
 * socialRentAdjustments: socialRentAdjustments,
 * housePriceIndex: 100000,
 * incomePerPersonYearly: 30000,
 * gasBillYearly: 700,
 * property: createTestProperty(),
 * forecastParameters: DEFAULT_FORECAST_PARAMETERS,`
 * @returns 
 */
export const createTestHousehold = (overrides = {}) => {
  return new Household ({
    averageRentYearly: 1200,
    socialRentAverageEarning: 354.1,
    socialRentAdjustments: socialRentAdjustments,
    housePriceIndex: 100000,
    incomePerPersonYearly: 30000,
    gasBillYearly: 700,
    property: createTestProperty(),
    forecastParameters: DEFAULT_FORECAST_PARAMETERS,
    ...overrides
  })
}

/**
 * Creates a simplified instance of the `Property` class with straightforward property values for testing. Assumes newbuild!
 * @param overrides Include custom values to overwrite those provided. Default values are:
 * `postcode: "SE15 1TX",
 * houseType: "T",
 * numberOfBedrooms: 2,
 * age: 1,
 * size: 88,
 * maintenancePercentage: .015,
 * newBuildPricePerMetre: 2120,
 * averageMarketPrice: 500000,
 * itl3: "TLI44",`
 * @returns
 */
export const createTestProperty = (overrides = {}) => {
    return new Property ({ 
      postcode: "SE15 1TX",
      houseType: "T",
      numberOfBedrooms: 2,
      age: 1,
      size: 88,
      maintenancePercentage: .015,
      newBuildPricePerMetre: 2120,
      averageMarketPrice: 500000,
      itl3: "TLI44",
      ...overrides
    })
}

/**
 * Creates a simplified instance of the `Mortgage` class, representing the land portion of `MarketPurchase`. Uses straightforward property values for testing. 
 * @param overrides Include custom values to overwrite those provided. Default values are:
 * `propertyValue: 300000,
 * interestRate: 0.035, 
 * mortgageTerm: 25,
 * initialDeposit: 0.1,` 
 * @returns 
 */
export const createTestMarketLandMortgage = (overrides = {}) => {
    return new Mortgage({
      propertyValue: 300000,
      interestRate: 0.035, 
      mortgageTerm: 25,
      initialDeposit: 0.1, 
      ...overrides
    });
  };

/**
 * Creates a simplified instance of the `Mortgage` class, representing the replacement value of a newbuild house in `MarketPurchase`. Uses straightforward property values for testing. 
 * @param overrides Include custom values to overwrite those provided. Default values are:
 * `propertyValue: 200000,
 * interestRate: 0.035, 
 * mortgageTerm: 25,
 * initialDeposit: 0.1,`
 * @returns 
 */
export const createTestNewbuildHouseMortgage = (overrides = {}) => {
  return new Mortgage({
    propertyValue: 200000,
    interestRate: 0.035, 
    mortgageTerm: 25,
    initialDeposit: 0.1, 
    ...overrides
  });
};

/**
 * Creates a simplified instance of the `Mortgage` class, representing the depreciated house portion of the Fairhold tenures. Uses straightforward property values for testing. 
 * @param overrides Include custom values to overwrite those provided. Default values are:
 * `propertyValue: 150000,
 * interestRate: 0.035, 
 * mortgageTerm: 25,
 * initialDeposit: 0.1, `
 * @returns 
 */
export const createTestDepreciatedHouseMortgage = (overrides = {}) => {
  return new Mortgage({
    propertyValue: 150000,
    interestRate: 0.035, 
    mortgageTerm: 25,
    initialDeposit: 0.1, 
    ...overrides
  });
};

/**
 * Creates a simplified instance of the `Mortgage` class, representing the discounted sale of a `FairholdLandPurchase` lease. Uses straightforward property values for testing. 
 * @param overrides Include custom values to overwrite those provided. Default values are:
 * `propertyValue: 45000,
 * interestRate: 0.035, 
 * mortgageTerm: 25,
 * initialDeposit: 0.1,`
 * @returns 
 */
export const createTestFairholdLandMortgage = (overrides = {}) => {
  return new Mortgage({
    propertyValue: 45000,
    interestRate: 0.035, 
    mortgageTerm: 25,
    initialDeposit: 0.1, 
    ...overrides
  });
};

/** 
 * Note that this creates an instance of `Fairhold`, not `FairholdLandPurchase`. The former is needed as part of the latter.
 * @param overrides Include custom values to overwrite those provided. Default values are:
 * `affordability: .6,
 *  landPriceOrRent: 300000,`
 * @returns
 */
export const createTestFairholdForLandPurchase = (overrides = {}) => {
  return new Fairhold({
    affordability: .6,
    landPriceOrRent: 300000,
    ...overrides
  })
}

/** 
 * Note that this creates an instance of `Fairhold`, not `FairholdLandRent`. The former is needed as part of the latter.
 * @param overrides Include custom values to overwrite those provided. Default values are:
 * `affordability: .6,
 *  landPriceOrRent: 900,`
 * @returns
 */
export const createTestFairholdForLandRent = (overrides = {}) => {
  return new Fairhold({
    affordability: .6,
    landPriceOrRent: 900,
    ...overrides
  })
}

/**
 * Creates a simplified instance of the `MarketPurchase` class. Uses straightforward property values for testing. 
 * @param overrides Include custom values to overwrite those provided. Default values are:
 * `averagePrice: 500000,
 * newBuildPrice: 200000,
 * depreciatedBuildPrice: 150000,
 * landPrice: 300000,
 * incomeYearly: 30000,
 * forecastParameters: DEFAULT_FORECAST_PARAMETERS,`
 * @returns 
 */
export const createTestMarketPurchase = (overrides = {}) => {
  return new MarketPurchase({
    averagePrice: 500000,
    newBuildPrice: 200000,
    depreciatedBuildPrice: 150000,
    landPrice: 300000,
    incomeYearly: 30000,
    forecastParameters: DEFAULT_FORECAST_PARAMETERS,
    ...overrides
  })
}

/**
 * Creates a simplified instance of the `MarketRent` class. Uses straightforward property values for testing. 
 * @param overrides Include custom values to overwrite those provided. Default values are:
 * `averageRentYearly: 18000,
 * averagePrice: 500000,
 * newBuildPrice: 200000,
 * depreciatedBuildPrice: 150000,
 * landPrice: 300000,
 * incomeYearly: 30000,
 * forecastParameters: DEFAULT_FORECAST_PARAMETERS,`
 * @returns 
 */
export const createTestMarketRent = (overrides = {}) => {
  return new MarketRent({
    averageRentYearly: 18000,
    averagePrice: 500000,
    newBuildPrice: 200000,
    depreciatedBuildPrice: 150000,
    landPrice: 300000,
    incomeYearly: 30000,
    forecastParameters: DEFAULT_FORECAST_PARAMETERS,
    ...overrides
  })
}

/**
 * Creates a simplified instance of the `FairholdLandPurchase` class, which includes `Fairhold` and `MarketPurchase` instances. Uses straightforward property values for testing. 
 * @param overrides Include custom values to overwrite those provided. Default values are:
 * `newBuildPrice: 200000,
 * depreciatedBuildPrice: 150000,
 * affordability: .6,
 * fairhold: createTestFairholdForLandPurchase(),
 * forecastParameters: DEFAULT_FORECAST_PARAMETERS,
 * marketPurchase: createTestMarketPurchase(),`
 * @returns 
 */
export const createTestFairholdLandPurchase = (overrides = {}) => {
  return new FairholdLandPurchase({
    newBuildPrice: 200000,
    depreciatedBuildPrice: 150000,
    affordability: .6,
    fairhold: createTestFairholdForLandPurchase(),
    forecastParameters: DEFAULT_FORECAST_PARAMETERS,
    marketPurchase: createTestMarketPurchase(),
    ...overrides
  })
}

/**
 * Creates a simplified instance of the `FairholdLandRent` class, which includes `Fairhold` and `MarketPurchase` instances. Uses straightforward property values for testing. 
 * @param overrides Include custom values to overwrite those provided. Default values are:
 * `averageRentYearly: 18000,
 * averagePrice: 500000,
 * newBuildPrice: 200000,
 * depreciatedBuildPrice: 150000,
 * landPrice: 300000,
 * incomeYearly: 30000,
 * fairhold: createTestFairholdForLandRent(),
 * forecastParameters: DEFAULT_FORECAST_PARAMETERS,
 * marketPurchase: createTestMarketPurchase(),`
 * @returns 
 */
export const createTestFairholdLandRent = (overrides = {}) => {
  return new FairholdLandRent({
    averageRentYearly: 18000,
    averagePrice: 500000,
    newBuildPrice: 200000,
    depreciatedBuildPrice: 150000,
    landPrice: 300000,
    incomeYearly: 30000,
    fairhold: createTestFairholdForLandRent(),
    forecastParameters: DEFAULT_FORECAST_PARAMETERS,
    marketPurchase: createTestMarketPurchase(),
    ...overrides
  })
}

/**
 * Creates a simplified instance of the `SocialRent` class. Uses straightforward property values for testing. 
 * @param overrides Include custom values to overwrite those provided. Default values are:
 * `numberOfBedrooms: 2,
 * socialRentAverageEarning: 354.1,
 * socialRentAdjustments: socialRentAdjustments,
 * housePriceIndex: 100000,
 * landToTotalRatio: .6,`
 * @returns 
 */
export const createTestSocialRent = (overrides = {}) => {
  return new SocialRent({
    numberOfBedrooms: 2,
    socialRentAverageEarning: 354.1,
    socialRentAdjustments: socialRentAdjustments,
    housePriceIndex: 100000,
    landToTotalRatio: .6,
    ...overrides
  })
}

/**
 * Creates a simplified instance of the `Lifetime` class, which includes instances of all tenure classes and `Property` too. Uses straightforward property values for testing. 
 * @param overrides Include custom values to overwrite those provided. Default values are from other class helper functions and `DEFAULT_FORECAST_PARAMETERS`. 
 * @returns 
 */
export const createTestLifetime = (overrides = {}) => {
  return new Lifetime({
    marketPurchase: createTestMarketPurchase(),
    marketRent: createTestMarketRent(),
    fairholdLandPurchase: createTestFairholdLandPurchase(),
    fairholdLandRent: createTestFairholdLandRent(),
    property: createTestProperty(),
    propertyPriceGrowthPerYear: DEFAULT_FORECAST_PARAMETERS.propertyPriceGrowthPerYear,
    constructionPriceGrowthPerYear: DEFAULT_FORECAST_PARAMETERS.constructionPriceGrowthPerYear,
    rentGrowthPerYear: DEFAULT_FORECAST_PARAMETERS.rentGrowthPerYear,
    yearsForecast: DEFAULT_FORECAST_PARAMETERS.yearsForecast,
    maintenancePercentage: DEFAULT_FORECAST_PARAMETERS.maintenancePercentage,
    incomeGrowthPerYear: DEFAULT_FORECAST_PARAMETERS.incomeGrowthPerYear,
    affordabilityThresholdIncomePercentage: DEFAULT_FORECAST_PARAMETERS.affordabilityThresholdIncomePercentage,
    incomeYearly: 30000,
    ...overrides
  })
}