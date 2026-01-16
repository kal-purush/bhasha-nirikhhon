class GeneralUserProbabilities {
    constructor(sex, age, BMI, race, region, smoker, physicalInactivity) {
      this.sex = sex;
      this.age = age;
      this.BMI = BMI;
      this.race = race;
      this.region = region;
      this.smoker = smoker;
      this.physicalInactivity = physicalInactivity;
    }
  }
// User object
class User {
    constructor(sex, age, BMI, race, region, smoker, physicalInactivity) {
      this.sex = sex;
      this.age = age;
      this.BMI = BMI;
      this.race = race;
      this.region = region;
      this.smoker = smoker;
      this.physicalInactivity = physicalInactivity;
    }
  }
  
  // Disease object
  class Disease {
    constructor(diseaseName, basic_prob, cond_sex, cond_age, cond_BMI, cond_race, cond_region, cond_smoker, cond_physicalInactivity) {
      this.name = diseaseName;
      this.basic_prob = basic_prob;
      this.cond_sex = cond_sex;
      this.cond_age = cond_age;
      this.cond_BMI = cond_BMI;
      this.cond_race = cond_race;
      this.cond_region = cond_region;
      this.cond_smoker = cond_smoker;
      this.cond_physicalInactivity = cond_physicalInactivity;
    }
  }
  // Initialize disease data
function initializeDiseaseData(){

    const diabetes = new Disease('diabetes', 13.2, [14.2, 12.4], [12.6, 30.2, 38.1], [27.7, 45.8, 27.7], [11.2,17.6,16.8,"X",16.4,14.5], [7.838,9.058,8.563,11.564],[13.8,19.8,37.1],[34.3,"X"]);
    const cancerAny = new Disease('cancerAny', 9.8, [9.0, 10.5], [2.2, 9.4, 26.8], [27.7, 45.8, 27.7], [11.7, 3.6, 5.3, "X", 3.4, 12.4], [9.3, 9.8, 10.3, 9.8], ["X", "X", "X"], ["X","X"]);
    const brainAndOtherNervousSystemCancer = new Disease(' brainAndOtherNervousSystemCancer', 0.055, [0.06, 0.05], ["X", "X", "X"], ["X", "X", "X"], [0.05, "X", "X", "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X","X"]);
    const breastCancer = new Disease('breastCancer', 1.7, [0.01, 3.3], [0.2, 1.6, 5.3],["X", "X", "X"],[1.9, 1.4, 0.6, 1.4, 1.7, 1.3], [1.6, 1.7, 2.2, 1.6], ["X", "X", "X"], ["X", "X"]);
    const colonAndRectumCancer = new Disease('colonAndRectumCancer', 0.42, [0.43, 0.41], ["X", "X", "X"], ["X", "X", "X"], [0.44, 0.33, "X", "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const nos = new Disease('nos', 0.24, [0, 0.49], ["X", "X", "X"], ["X", "X", "X"], [0.55, 0.24, "X", "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const esophagusCancer = new Disease('esophagusCancer', 0.02, [0.02, 0.01], ["X", "X", "X"], ["X", "X", "X"], [0.02, 0.01, "X", "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const hodgkinLymphoma = new Disease('hodgkinLymphoma', 0.07, [0.07, 0.06], ["X", "X", "X"], ["X", "X", "X"], [0.08, 0.05, "X", "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const kidneyAndRenalPelvisCancer = new Disease('kidneyAndRenalPelvisCancer', 0.18, [0.22, 0.14], ["X", "X", "X"], ["X", "X", "X"], [0.19, 0.15, "X", "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const larynxCancer = new Disease('larynxCancer', 0.03, [0.05, 0.01], ["X", "X", "X"], ["X", "X", "X"],[0.03, 0.04, "X", "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const leukemia = new Disease('leukemia', 0.14, [0.16, 0.12], ["X", "X", "X"], ["X", "X", "X"], [0.16, 0.08, "X", "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const lungAndBronchusCancer = new Disease('lungAndBronchusCancer', 0.18, [0.16, 0.2], ["X", "X", "X"], ["X", "X", "X"], [0.19, 0.14, "X", "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const skinCancerAny = new Disease('skinCancerAny', 3.3, [3.4, 3.2], [0.5, 3.1, 9.85], ["X", "X", "X"], [4.5, 0.3, 0.1, 0.3, 0.0, 0.8], [3.4, 3.1, 3.3, 3.4], ["X", "X", "X"], ["X", "X"]);
    const nonHodgkinLymphoma = new Disease('nonHodgkinLymphoma', 0.23, [0.25, 0.21], ["X", "X", "X"], ["X", "X", "X"], [0.26, "X", 0.13, "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const oralCavityAndPharynxCancer = new Disease('oralCavityAndPharynxCancer', 0.12, [0.17, 0.08], ["X", "X", "X"], ["X", "X", "X"], [0.14, "X", 0.06, "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const ovaryCancer = new Disease('ovaryCancer', 0.07, [0, 0.14], ["X", "X", "X"], ["X", "X", "X"], [0.16, "X", 0.08, "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const pancreasCancer = new Disease('pancreasCancer', 0.03, [0.03, 0.03], ["X", "X", "X"], ["X", "X", "X"], [0.03, "X", 0.02, "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const prostateCancer = new Disease('prostateCancer', 2.4, [2.4, 0], [0, 1.5, 10.25], ["X", "X", "X"], [2.6, 1.1, 2.9, 1.1, 0.8, "X"], [2.0, 2.6, 2.6, 2.6], ["X", "X", "X"], ["X", "X"]);
    const stomachCancer = new Disease('stomachCancer', 0.04, [0.04, 0.03], ["X", "X", "X"], ["X", "X", "X"], [0.04, "X", 0.04, "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const thyroidCancer = new Disease('thyroidCancer', 0.27, [0.13, 0.42], ["X", "X", "X"], ["X", "X", "X"], [0.30, "X", 0.13, "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const urinaryBladderCancer = new Disease('urinaryBladderCancer', 0.22, [0.34, 0.11], ["X", "X", "X"], ["X", "X", "X"], [0.26, "X", 0.08, "X", "X", "X"], ["X", "X", "X", "X"], ["X", "X", "X"], ["X", "X"]);
    const depression = new Disease('anxietyAndPanicDisorder', 11.3, [8.4, 14.1], [13.8, 10.9, 7], ["X", "X", "X"], [12.2, 9.8, 18.4, 9.1, 5.5, 17.2], [11.7, 11.3, 11, 11.3], ["X", "X", "X"], [11.9, "X"]);
    const anxietyAndPanicDisorder = new Disease('depression', 4.5, [3.7, 5.2], [4.8, 4.7, 3.6], ["X", "X", "X"], [4.6, 3.5, 10.3, 4.2, 2.5, "X"], [4, 4.3, 4, 5.1], ["X", "X", "X"], [6.5, "X"]);
    const arthritis = new Disease('arthritis', 21.3, [17.8, 24.5], [5, 25.9, 27.7], ["X", "X", "X"], [23.3, 20.5, 12.8, 12.9, 10.8, 12.1], [18.1, 23.7, 21.1, 22.0], ["X", "X", "X"], ["X", "X"]);
    const heartAttack = new Disease('heartAttack', 3, [2, 4.1], [0.5, 3.4, 8.2], ["X", "X", "X"], [3.3, 3.1, 2.0, 2.2, 1.4, "X"], [2.1, 3.6, 2.3, 3.7], ["X", "X", "X"], ["X", "X"]);
    const diagnosedHypertension = new Disease('diagnosedHypertension', 26.9, [27.7, 26.1], [3.9, 15.1, 47.85], ["X", "X", "X"], [27.3, 34.5, 18.8, 21.2, 20.2, 25.4], [22, 27.5, 25.6, 30.2], ["X", "X", "X"], ["X", "X"]);
    const highCholesterol = new Disease('highCholesterol', 21.6, [22.7, 20.7], [5.4, 27.7, 30.5], ["X", "X", "X"], [22.6, 26, 18, 25.1, 22.2, 22.8], [20.3, 25.5, 19.9, 23.8], ["X", "X", "X"], ["X", "X"]);
    const coronaryHeartDisease = new Disease('coronaryHeartDisease', 4.9, [6.3, 3.6], [0.5, 4.7, 15.25], ["X", "X", "X"], [5.3, 3, 5, 3.3, 2.9, 8.3], [3.8, 5, 4.6, 5.7], ["X", "X", "X"], ["X", "X"]);    
    const syphilis = new Disease('syphilis', 0.0515, [0.0244, 0.007], [0.03144, "X", "X"], ["X", "X", "X"], [0.0087, 0.0173, 0.0406, 0.0169, 0.0047, 0.0422], [0.0212, 0.0124, 0.0124, 0.0158], ["X", "X", "X"], ["X", "X"]);
    const chlamydia = new Disease('chlamydia', 0.4906, [0.3517, 0.618], [1.18755, "X", "X"], ["X", "X", "X"], [0.1795, 0.3491, 1.0516, 0.2659, 0.0945, 0.6409], [0.4802, 0.4725, 0.4179, 0.5396], ["X", "X", "X"], ["X", "X"]);
    const gonorrhea = new Disease('gonorrhea', 0.2099, [0.2442, 0.1731], [0.52359, "X", "X"], ["X", "X", "X"], [0.0767, 0.1374, 0.6274, 0.1524, 0.0376, 0.3573], [0.2096, 0.2106, 0.1522, 0.2357], ["X", "X", "X"], ["X", "X"]);
    const alzheimersDisease = new Disease('alzheimersDisease', 8.5, [8.2, 8.7], [6.4, 7.1, 10.1], ["X", "X", "X"], [8.3, 10.7, 7.7, 3.4, 3.4, 15.1], [8.7, 8.3, 7.9, 9.2], ["X", "X", "X"], ["X", "X"]);


    return [diabetes, cancerAny,  brainAndOtherNervousSystemCancer, breastCancer, colonAndRectumCancer, nos, esophagusCancer, hodgkinLymphoma, kidneyAndRenalPelvisCancer, larynxCancer, leukemia, lungAndBronchusCancer, skinCancerAny, nonHodgkinLymphoma, oralCavityAndPharynxCancer, syphilis, ovaryCancer, pancreasCancer, prostateCancer, stomachCancer, thyroidCancer, urinaryBladderCancer, anxietyAndPanicDisorder, depression, arthritis, heartAttack, diagnosedHypertension, highCholesterol, coronaryHeartDisease, chlamydia, gonorrhea, alzheimersDisease];
  }
  // Calculate disease probability
  function calculateProbability(user, disease, userProbabilities) {
    let numerator = disease.basic_prob;
    let denominator = 1;
    
    if (disease.cond_sex[user.sex] !== 'X') {
      numerator *= disease.cond_sex[user.sex];
      denominator *= userProbabilities.sex[user.sex];
    }
    if (disease.cond_age[user.age] !== 'X') {
      numerator *= disease.cond_age[user.age];
      denominator *= userProbabilities.age[user.age];
    }
    if (disease.cond_BMI[user.BMI] !== 'X') {
      numerator *= disease.cond_BMI[user.BMI];
      denominator *= userProbabilities.BMI[user.BMI];
    }
    if (disease.cond_race[user.race] !== 'X') {
      numerator *= disease.cond_race[user.race];
      denominator *= userProbabilities.race[user.race];
    }
    if (disease.cond_region[user.region] !== 'X') {
      numerator *= disease.cond_region[user.region];
      denominator *= userProbabilities.region[user.region];
    }
    if (disease.cond_smoker[user.smoker] !== 'X') {
      numerator *= disease.cond_smoker[user.smoker];
      denominator *= userProbabilities.smoker[user.smoker];
    }
    if (disease.cond_physicalInactivity[user.physicalInactivity] !== 'X') {
      numerator *= disease.cond_physicalInactivity[user.physicalInactivity];
      denominator *= userProbabilities.physicalInactivity[user.physicalInactivity];
    }

    return (numerator / denominator)/100;
  }

  
  // Calculate probabilities for all diseases
  function calculateDiseaseProbabilities({ sex, age, bmi, race, region, smokerLevel, physicalInactivity }) {
    const user = new User(sex, age, bmi, race, region, smokerLevel, physicalInactivity);
    const userProbabilities = new GeneralUserProbabilities([49.5,50.5],[60.3,25.4, 16.4],[30.7,42.4, 9.2],[59.3,18.9,12.6,2.3,5.9,0.2],[0.237,0.2074,0.1722,0.3833],[88.8,11.2,6.744],[25.5,74.5])

    // Initialize disease data
    const diseases = initializeDiseaseData();
    
    let probabilities = {};
  
    for (let disease of diseases) {
            probabilities[disease.name] = calculateProbability(user, disease, userProbabilities);
        }
    
    return Object.entries(probabilities)
    .sort(([,a],[,b]) => -(a-b))
    .reduce((r, [k, v]) => ({ ...r, [k]: v }), {});



  }

export default { calculateDiseaseProbabilities }

    /* Gameplan: 

    
    1. create object for user and each disease
    diseases = [
        Diabetes,
        CancerAny,
        Brain_and_Other_Nervous_System,Breast,
        Colon_and_Rectum, 
        NOS, 
        Esophagus, 
        Hodgkin_Lymphoma, 
        Kidney_and_Renal_Pelvis, 
        Larynx, 
        Leukemia, 
        Lung_and_Bronchus, 
        SkinCancerAny, 
        Non-Hodgkin_Lymphoma, 
        Oral_Cavity_and_Pharynx, 
        Ovary, 
        Pancreas, 
        Stomach, 
        Thyroid, 
        Urinary_Bladder, 
        MentalHealth1, 
        MentalHealth2, 
        Obesity, 
        Arthritis, 
        HeartAttack, 
        DiagnosedHypedtension, 
        HighCholesterol, 
        CoronaryHeartDisease, 
        Syphilis, 
        Chlamydia, 
        Gonorrhea
    ]

    user variables:
     - sex = [male, female] // either 0 or 1
     - age = [ageCat1, ageCat2, ageCat3] // either 0, 1 or 2
     - BMI = [BMICat1, BMICat2, BMICat3] // either 0, 1 or 2
     - race = [white, hispanic, black, other, asian, nativePacificIslander] // either 0, 1, 2, 3, 4, or 5
     - region = [W, NE, S, MW] // either 0, 1, 2, or 3
     - smoker = [smokerLevel1, smokerLevel2, _smokerLevel3 // either 0, 1 or 2
     - physicalInactivity [yes, no] // either 0 or 1

    disease variables: 
     - basic_prob
     - cond_sex = [cond_male, cond_female]
     - cond_age = [cond_ageCat1, cond_ageCat2, cond_ageCat3]
     - cond_BMI = [cond_BMICat1, cond_BMICat2, cond_BMICat3]
     - cond_race = [cond_white, cond_hispanic, cond_black, cond_other, cond_asian, cond_nativePacificIslander]
     - cond_region = [cond_W, cond_NE, cond_S, cond_MW]
     - cond_smoker = [cond_smokerLevel1, cond_smokerLevel2, cond__smokerLevel3
     - cond_physicalInactivity

     2. Initialize objects for each disease with data.json, whenever we don't have data for a variable -> we put "X"

     3. once user requests we calculate conditional probability based on input parameters for any given disease asks for certain input data  

     double probability_diabetes(user_data){

        
     }

    */