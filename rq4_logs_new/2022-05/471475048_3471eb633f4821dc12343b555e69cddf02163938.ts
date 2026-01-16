import DateOptionParameter from "@/structures/OptionParameters/DateOptionParameter";
import NumberOptionParameter from "@/structures/OptionParameters/NumberOptionParameter";

enum OptionParameters {
    S = 'S',
    K = 'K',
    sigma = 'sigma',
    r = 'r',
    T = 'T',
}

new NumberOptionParameter({
    name: OptionParameters.S,
    label: 'S',
    description: 'Значение рискового актива в начальный момент времени',
    defaultValue: 100,
    step: 10,
    min: 0
})
new NumberOptionParameter({
    name: OptionParameters.K,
    label: 'K',
    description: 'Контрактная цена',
    defaultValue: 100,
    step: 10,
})
new NumberOptionParameter({
    name: OptionParameters.sigma,
    label: '\u03C3',
    description: 'Волатильность',
    defaultValue: 0.1,
    max: 1,
    min: 0,
    step: 0.1
})
new NumberOptionParameter({
    name: OptionParameters.r,
    label: 'r',
    description: 'Безрисковая процентная ставка за период ∆t',
    defaultValue: 0.1,
    max: 1,
    min: 0,
    step: 0.1
})
new DateOptionParameter({
    name: OptionParameters.T,
    label: 'T',
    description: 'Дата погашения',
    defaultValue: new Date(
        new Date().getFullYear(),
        new Date().getMonth(),
        new Date().getDate() + 90
    ),
    min: new Date()
})

export default OptionParameters