export const ohCanada = {
    languageTag: 'en-CA',
    languageName: 'English (Canadian)',
    delimiters: {
        thousands: ' ',
        decimal: ',',
    },
    abbreviations: {
        thousand: 'k',
        million: 'm',
        billion: 'b',
        trillion: 't',
    },
    ordinal: (number) => {
        const b = number % 10;
        return (~~(number % 100 / 10) === 1) ? 'th' : (b === 1) ? 'st' : (b === 2) ? 'nd' : (b === 3) ? 'rd' : 'th';
    },
    currency: {
        symbol: '$',
        position: 'postfix',
        code: 'CAD',
    },
    currencyFormat: {
        thousandSeparated: true,
        totalLength: 4,
        spaceSeparated: true,
        average: true,
    },
    formats: {
        fourDigits: {
            totalLength: 4,
            spaceSeparated: true,
            average: true,
        },
        fullWithTwoDecimals: {
            output: 'currency',
            mantissa: 2,
            spaceSeparated: true,
            thousandSeparated: true,
        },
        fullWithTwoDecimalsNoCurrency: {
            mantissa: 2,
            thousandSeparated: true,
        },
        fullWithNoDecimals: {
            output: 'currency',
            spaceSeparated: true,
            thousandSeparated: true,
            mantissa: 0,
        },
    },
};