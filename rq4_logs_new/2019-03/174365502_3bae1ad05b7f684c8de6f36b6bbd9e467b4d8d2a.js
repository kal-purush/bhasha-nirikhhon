var salesTaxRates = {
  AB: 0.05,
  BC: 0.12,
  SK: 0.10
};

var companySalesData = [
  {
    name: "Telus",
    province: "BC",
    sales: [ 100, 200, 400 ]
  },
  {
    name: "Bombardier",
    province: "AB",
    sales: [ 80, 20, 10, 100, 90, 500 ]
  },
  {
    name: "Telus",
    province: "SK",
    sales: [ 500, 100 ]
  }
];

function calculateSalesTax(companyData, taxData) {
  var output = { };

  companyData.forEach((company) => {
    var salesSum = company.sales.reduce((sum, num) => sum + num);
    var taxSum = taxData[company.province] * salesSum;

    // if it exists, calculate sum.
    // else, create object and set sum.
    if (output[company.name] && output[company.name].totalSales) {
      output[company.name].totalSales += salesSum;
      output[company.name].totalTaxes += taxSum;
    } else {
      output[company.name] = { totalSales: salesSum, totalTaxes: taxSum };
    }

  })

  return output;
}

var results = calculateSalesTax(companySalesData, salesTaxRates);
console.log(results);

/* Expected Results:
{
  Telus: {
    totalSales: 1300,
    totalTaxes: 144
  },
  Bombardier: {
    totalSales: 800,
    totalTaxes: 40
  }
}
*/