/**
 * @ Author: Aailson Barbosa Ribeiro <ribeiro@alailson.com.br>
 * @ Create Time: 2020-12-23 13:20:41
 * @ I love do this ♡ Enlightened by God
 * @ Modified by: Your name
 * @ Modified time: 2021-01-13 10:49:31
 */
/*
CNPJ.js Verifica se é CNPJ  */

module.exports = (cnpj) => {

    cnpj = cnpj.replace(/[^\d]+/g, '');

    if (cnpj == '') return false;

    if (cnpj.length != 14)
        return false;

    // Elimina CNPJs invalidos conhecidos
    if (cnpj == "00000000000000" ||
        cnpj == "11111111111111" ||
        cnpj == "22222222222222" ||
        cnpj == "33333333333333" ||
        cnpj == "44444444444444" ||
        cnpj == "55555555555555" ||
        cnpj == "66666666666666" ||
        cnpj == "77777777777777" ||
        cnpj == "88888888888888" ||
        cnpj == "99999999999999")
        return false;

    // Valida DVs
    let result = 0,
        length = cnpj.length - 2,
        number = cnpj.substring(0, length),
        digits = cnpj.substring(length),
        stance = length - 7,
        calc = 0;

    for (i = length; i >= 1; i--) {
        calc += number.charAt(length - i) * stance--;
        if (stance < 2)
            stance = 9;
    }

    result = calc % 11 < 2 ? 0 : 11 - calc % 11;
    if (result != digits.charAt(0))
        return false;

    length = length + 1;
    number = cnpj.substring(0, length);
    calc = 0;
    stance = length - 7;
    for (i = length; i >= 1; i--) {
        calc += number.charAt(length - i) * stance--;
        if (stance < 2)
            stance = 9;
    }
    result = calc % 11 < 2 ? 0 : 11 - calc % 11;
    if (result != digits.charAt(1))
        return false;

    return true;

}

