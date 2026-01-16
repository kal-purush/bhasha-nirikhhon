var decodeBits = function(bits){
    // ToDo: Accept 0's and 1's, return dots, dashes and spaces
    //1-eliminar ceros de los extremos
    //leer secuencia de caracteres y reemplazar:
    //···· · −·−−   ·−−− ··− −·· ·
    //11--->punto de letra
    //00--->espacio de letra
    //000000--->espacio de letras en palabra
    //111111--->barra de letra
    return bits.replace('111', '-').replace('000', ' ').replace('1', '.').replace('0', '');
}

var decodeMorse = function(morseCode){
    // ToDo: Accept dots, dashes and spaces, return human-readable message
    return morseCode.replace('.', MORSE_CODE['.']).replace('-', MORSE_CODE['-']).replace(' ', '');
}

('1100110011001100000011000000111111001100111111001111110000000000000011001111110011111100111111000000110011001111110000001111110011001100000011')