function hirisplex(genotypes) {
  var alpha1 = 1.70;
  var alpha2 = -1.26;
  var alpha3 = -2.62;

  var beta1 = [1.11, 0, 46, -1.75, -1.29, 0.39, 0.77, -1.69, 0.10, 0.49, -0.48, 0.69, -0.82, -0.18];
  var beta2 = [0.55, 0.55, 0.10, -1.15, 0.30, 0.85, -13.89, 0.58, -1.14, -0.09, 0.01, -11.78, -0.16];
  var beta3 = [4.09, 0.61, -2.49, -1.13, 1.20, 1.15, 0.10, -0.02, 0.19, -0.54, 0.87, -3.48, 0.40];

  var sum_b1 = 0;
  var sum_b2 = 0;
  var sum_b3 = 0;
  
  var snps = ['MC1R_R', 'MC1R_r', 'rs12913832', 'rs12203592', 'rs1042602', 'rs4959270', 'rs28777', 'rs683', 'rs1800407', 'rs2402130', 'rs12821256', 'rs16891982', 'rs2378249'];

  for (var i = 0; i < snps.length; i++) {
    sum_b1 += beta1[i] * genotypes[i];
    sum_b2 += beta2[i] * genotypes[i];
    sum_b3 += beta3[i] * genotypes[i];
  }

  var exp_b1 = Math.exp(alpha1 + sum_b1)
  var exp_b2 = Math.exp(alpha2 + sum_b2)
  var exp_b3 = Math.exp(alpha3 + sum_b3)
  var pBlond = exp_b1 / (1 + exp_b1 + exp_b2 + exp_b3)
  var pBrown = exp_b2 / (1 + exp_b1 + exp_b2 + exp_b3)
  var pRed = exp_b3 / (1 + exp_b1 + exp_b2 + exp_b3)
  var pBlack = 1 - pBlond - pBrown - pRed

  return [pBlond, pBrown, pRed, pBlack];
}

module.exports = { hirisplex };