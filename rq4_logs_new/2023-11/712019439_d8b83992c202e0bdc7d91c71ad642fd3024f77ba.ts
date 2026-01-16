import axios from 'services/axios'

export default async (): Promise<[any, any]> => {
    return new Promise(async (resolve, reject) => {
        const INDEX_DATA = await axios.get('/public/index_data.json')
        const ACTION_DATA = await axios.get('/public/action_data.json')
        if (INDEX_DATA.status !== 200) {
            reject([])
        }
        resolve([INDEX_DATA.data, ACTION_DATA.data])
    })
}
setInterval(() => {}, 300000)

// export const INDEX_DATA = [
//     ['RUGBICP10Y', 'Государственные облигации 5-10 лет'],
//     ['RUSFAR', 'Справедливая стоимость денег'],
//     ['EPSI', 'Пенсионные накопления (акции + облигации)'],
//     ['BPSIG', 'Пенсионные накопления (ОФЗ)'],
//     ['RVI', 'Волатильность'],
//     ['RUCGI', 'Корпоративное управление'],
//     ['CREI', 'Складская недвижимость'],
//     ['MREDC', 'Московская недвижимость ДомКлик'],
//     ['DOMMBSTR', 'Ипотечные облигации ДОМ РФ'],
//     ['RUCBHYCP', 'Высокодоходные Облигаций (ВДО)'],
//     ['RUGROWTR', 'Облигации малого и среднего бизнеса (МСП)'],
//     ['MOEXOG', 'Нефть и газ'],
//     ['MOEXEU', 'Энергетика'],
//     ['MOEXTL', 'Телекомы'],
//     ['MOEXMM', 'Металлодобыча'],
//     ['MOEXFN', 'Финансы'],
//     ['MOEXCN', 'Потребители'],
//     ['MOEXCH', 'Нефтехимия'],
//     ['MOEXTN', 'Транспорт'],
//     ['MOEXIT', 'IT-сфера'],
//     ['MOEXRE', 'Строители'],
// ]
// export const ACTION_DATA = ['+', '-', '*', '/']