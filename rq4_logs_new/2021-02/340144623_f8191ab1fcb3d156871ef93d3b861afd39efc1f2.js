const comands = [];
for (let i = 0; i < Infinity; i++) {
    let a = prompt('Введите команду (add, del, stop)', 'add, Иван');
    comands.push(a)
    if (a=='stop') {
        break;
    }
    else if (a == "del") {
        let del = prompt('Какое имя удалит');
        for (let j = 0; j < comands.length; j++) {
            if(comands[j]==del){
                comands[j].splice()
            }
        }
    }
}
console.log(comands);