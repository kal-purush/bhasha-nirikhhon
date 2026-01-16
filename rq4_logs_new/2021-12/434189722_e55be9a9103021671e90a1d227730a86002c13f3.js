
const _inputs = [[0, 0], [0, 1], [1, 0], [1, 1]]
const _outputs = [0, 1, 1, 1]
let _weights = [0.3, 0.1]
const _learningRate = 0.002

// Verirfica se o neurônio vai ativar
function stepFunction(sumOfInputs) {
    if (sumOfInputs >= 1)
        return 1
    return 0
}

// Faz a multiplicação das entrasdas (linhas) pelos pesos e depois soma [x, y]
function calculateOutput(row, weights) {
    let sumOfEntries = 0.0
    row.forEach((value, index) => {
        sumOfEntries += value * weights[index]
    });
    return sumOfEntries
}

// Atualiza os pesos com base no erro
// Caucula o erro
function updateWeights(outputs, indexOne, calculatedOutput, weights, learningRate, inputs) {
    let error = outputs[indexOne] - calculatedOutput
    let newWeights = []
    weights.forEach((weight, indexTwo)=> {
        newWeights.push(weight + (learningRate * inputs[indexOne][indexTwo] * error))        
    });
    return  { error, newWeights }
}
 

function trainRNA() {
    let totalError
    while (totalError != 0) {
        totalError = 0
        _inputs.forEach((row, indexOne) => {
           let result =  updateWeights(_outputs, indexOne, 
                stepFunction(calculateOutput(row, _weights)), _weights, _learningRate, _inputs)
                _weights = result.newWeights
                totalError += result.error
        });
    }
}

trainRNA();
console.log(stepFunction(calculateOutput(_inputs[0], _weights)));
console.log(stepFunction(calculateOutput(_inputs[1], _weights)));
console.log(stepFunction(calculateOutput(_inputs[2], _weights)));
console.log(stepFunction(calculateOutput(_inputs[3], _weights)));
