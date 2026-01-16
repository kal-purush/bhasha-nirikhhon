let nam1 = 'Digite o nome do personagem 1: '
let power1 = 'Digite o poder de ataque do personagem 1: '
let nam2 = 'Digite o nome do personagem 2: '
let healthPoints2 = 'Digite a vida do personagem 2: '
let defense2 = 'Digite a defesa do personagem 2: '
let shield2 = 'O personagem 2 possui escudo?'

if (shield2 == 'não' && power1 > defense2) {
  console.log(`${nam1} atacou. ${nam2} sofreu ${power1 - defense2} de dano e não possui escudo. O restante da sua vida é ${healthPoints2 - (power1 - defense2)}`)
} else if (shield2 == 'sim' && power1 > defense2) {;;
  console.log(`${nam1} atacou. ${nam2} sofreu ${(power1 - defense2)/2} de dano por possuir escudo. O restante da sua vida é ${healthPoints2 - ((power1 - defense2)/2)}.`)
} else {
  console.log(`${nam1} atacou, mas não afetou ${nam2}. Não houve dano!`)
}