var a = 16;
document.write(a);

document.write('Salem Alem');

var a = 5;
document.write((a+2)** 2);

var m = 22;
var n = 7;
document.write(m/n);

alert('Бұл js пен жазылған веб сайт')

a = 6
if (a / 2 == 0 && a / 7 ==0){
    document.write('YES')
}
else{
    document.write('NO')
}

a = confirm('18 жасқа толғансыз ба?')
if (a ==true){
    document.write('Қош келдіңіз')
}
else{
    document.write('Сізге бұл сайтқа кіруге болмайды')
}

var name = prompt('Атыңыз кім?')
if (name){
    document.write('Қош келдіңіз ' + name +"!")
}

var a = prompt('Санды еңгізініз');
if (a == 0){
    document.write(' 0-ге тең')
}
else{
    document.write('0-ге тең емес')
}


var a = prompt('Жұмыс тәжірибесіңіз қанша жыл?');
if(a <= 2){
    document.write('Кешіріңіз,тәжірибеңіз жеткіліксіз')
}
else if(a <= 4){
    document.write('Сізбен байланысамыз')
}
else{
    document.write('Сізді күтеміз!')
}