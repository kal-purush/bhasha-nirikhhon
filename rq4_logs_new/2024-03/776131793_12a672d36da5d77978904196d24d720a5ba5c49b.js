function makelead(yepp) {
    test = yepp;
    const item = player.pokemon.splice(yepp, 1)[0];
    player.pokemon.unshift(item);
    updateview()
}

function remove(yepp) {
    player.pokemon.splice(yepp, 1)
    updateview()
}

function leadcries() {
    battlemessage = player.name + ' sendte ut ' + player.pokemon[0].name + ' og ' + rival.name + ' sendte ut ' + rival.pokemon[0].name + '!'
    buttonsenabled = false
    updateview()
    setTimeout(function () {
        pokemonsound = sounds[player.pokemon[0].name.toLowerCase()];
        pokemonsound.play()
        pokemonsoundx = sounds[rival.pokemon[0].name.toLowerCase()];
        pokemonsoundx.play()
        setTimeout(function () {
            battlemessage = ''
            buttonsenabled = true
            updateview()
        }, 2000)
    }, 1000)
}

function battlemanager(playermove, when, para) {

    buttonsenabled = false
    updateview()
    turndecider = when
    if (playermove != 'foe' && playermove != 'friend') usermove = playermove

    if (player.pokemon[0].speed > rival.pokemon[0].speed) playerisfaster = true
    else playerisfaster = false
    if (buttonsenabled) turndecider = '0'
    if (playermove == 'potion' && when == '1') playerisfaster = true
    random1 = randomenemymove()
    if (random1 == 0) rivalsmove = rival.pokemon[0].move1
    if (random1 == 1) rivalsmove = rival.pokemon[0].move2
    if (random1 == 2) rivalsmove = rival.pokemon[0].move3
    if (random1 == 3) rivalsmove = rival.pokemon[0].move4

    if (usermove == Protect && turndecider > '0') {
        turndecider = '6'
        battlemessage = rival.pokemon[0].name + ' brukte ' + rivalsmove + '!'
        setTimeout(function () {
            battlemessage = player.pokemon[0].name + ' blokerte skaden!'
            updateview()
            setTimeout(function () {
                battlemanager('hva', '3')
            }, 2000)
        }, 3000)
    }


    random = randomaccuracy()
    random2 = randomaccuracy()

    if (usermove == Machpunch || usermove == Protect && turndecider == '0') playerisfaster = true
    if (rivalsmove == Machpunch || rivalsmove == Protect && turndecider == '0') playerisfaster = false
    if (usermove == Machpunch && rivalsmove == Machpunch && player.pokemon[0].speed > rival.pokemon[0].speed && turndecider == '0') playerisfaster = true
    if (usermove == Machpunch && rivalsmove == Machpunch && player.pokemon[0].speed <= rival.pokemon[0].speed && turndecider == '0') playerisfaster = false
    if (usermove == Machpunch && turndecider > '1') playerisfaster = true
    if (rivalsmove == Machpunch && turndecider > '2') playerisfaster = false

    if (rivalsmove == Protect && turndecider > '0' && playermove != 'hvorfor') {
        turndecider = '6'
        battlemessage = player.pokemon[0].name + ' brukte ' + usermove + '!'
        setTimeout(function () {
            battlemessage = rival.pokemon[0].name + ' blokerte skaden!'
            updateview()
            setTimeout(function () {
                battlemanager('hvorfor', '3')
            }, 2000)
        }, 3000)
    }

    if (playergigaimpactcd == 2) {
        turndecider = '6'
        battlemessage = player.pokemon[0].name + ' må hvile!'
        setTimeout(function () {
            playergigaimpactcd = 0
            battlemanager('kek', '1')
        }, 2000)
    }
    if (playergigaimpactcd == 1) playergigaimpactcd = 2


    if (playermove == 'kek') playerisfaster = true

    if (playerisfaster && turndecider == '0') {
        whoismoving('friend')
        usermove('friend', '1')
    }
    if (!playerisfaster && turndecider == '0') {
        whoismoving('foe')
        window[rivalsmove]('foe', '2')
    }
    if (player.pokemon[0].health <= 0) {
        turndecider = '5'
        deathmanager('friend')
    }
    if (rival.pokemon[0].health <= 0) {
        turndecider = '5'
        deathmanager('foe')
    }
    if (playerisfaster && turndecider == '1') {
        whoismoving('foe')
        window[rivalsmove]('foe', '3')
    }
    if (!playerisfaster && turndecider == '2') {
        whoismoving('friend')
        usermove('friend', '3')
    }
    if (player.pokemon[0].health <= 0 && turndecider != '5') {
        turndecider = '5'
        deathmanager('friend')
    }
    if (rival.pokemon[0].health <= 0 && turndecider != '5') {
        turndecider = '5'
        deathmanmager('foe')
    }
    if (turndecider == '3') endofturnevents()

    updateview()
}

function endofturnevents() {
    if (playerburned) {
        player.pokemon[0].health -= (player.pokemon[0].maxhealth * 0.125)
        battlemessage = player.pokemon[0].name + ' tok brannskade!'
        updateview()
    }
    else if (rivalburned) {
        rival.pokemon[0].health -= (rival.pokemon[0].maxhealth * 0.125)
        battlemessage = rival.pokemon[0].name + ' tok brannskade!'
        updateview()
    }
    else if (playerburned && rivalburned) {
        player.pokemon[0].health -= (player.pokemon[0].maxhealth * 0.125)
        rival.pokemon[0].health -= (rival.pokemon[0].maxhealth * 0.125)
        battlemessage = 'Begge tok brannskade'
        updateview()
    }
    else if (!playerburned && !rivalburned) {
        battlemessage = ''
        updateview()
    }
    if (rival.pokemon[0].health <= 0) deathmanager('foe')
    if (player.pokemon[0].health <= 0) deathmanager('friend')
    if (!playerburned && !rivalburned && player.pokemon[0].health >= 0 && rival.pokemon[0].health >= 0) {
        updateview()
    }
    updateview()
    setTimeout(function () {
        if (!rival.pokemon[0].health <= 0 && !player.pokemon[0].health <= 0) {
            battlemessage = ''
            buttonsenabled = true
            updateview()
        }
    }, 1500)
}

function deathmanager(who) {
    if (who == 'foe') {
        battlemessage = rival.pokemon[0].name + ' døde!'
        pokemonsound = sounds[rival.pokemon[0].name.toLowerCase()];
        pokemonsound.play()
        updateview()
        setTimeout(function () {
            pokemonsound = sounds[rival.pokemon[0].name.toLowerCase()];
            if (rival.pokemon.length == 9 || rival.pokemon.length == 7 || rival.pokemon.length == 5 || rival.pokemon.length == 3 || rival.pokemon.length == 1) {
                buttonsenabled = false
                changerival(rival.pokemon.length)
            }
            else {
                rival.pokemon.splice(0, 1)
                resetstats('foe')
                pokemonsound = sounds[rival.pokemon[0].name.toLowerCase()];
                pokemonsound.play()
                battlemessage = rival.name + ' sendte ut ' + rival.pokemon[0].name + '!'
                buttonsenabled = true
                updateview()
            }
            updateview()
        }, 2000)
    }
    if (who == 'friend') {
        battlemessage = player.pokemon[0].name + ' døde!'
        pokemonsound = sounds[player.pokemon[0].name.toLowerCase()];
        pokemonsound.play()
        resetstats('friend')
        updateview()
        setTimeout(function () {
            player.pokemon.splice(0, 1)
            if (player.pokemon.length != 0) {
                pokemonsound = sounds[player.pokemon[0].name.toLowerCase()];
                pokemonsound.play()
                battlemessage = player.name + ' sendte ut ' + player.pokemon[0].name + '!'
            }
            buttonsenabled = true
            updateview()
        }, 2000)
    }
}

function changerival(length) {
    if (length == 9) {
        rival.avatar = `<div style="position: fixed; top: 5%; right: 5%; width: 11vh; text-align: center">
        Bertha
        <img style="width: 14vh; height: auto" src="pictures/bertha.png" alt="">
        </div>
        `
        rival.name = 'Bertha'
        battlemessage = 'Du slo Aaron!. Neste utfordrer er Bertha'
        setTimeout(function () {
            rival.pokemon.splice(0, 1)
            battlemessage = rival.name + ' sendte ut ' + rival.pokemon[0].name + '!'
            pokemonsound = sounds[rival.pokemon[0].name.toLowerCase()];
            pokemonsound.play()
            resetstats('foe')
            updateview()
            setTimeout(function () {
                battlemessage = ''
                buttonsenabled = true
                updateview()
            }, 2000)
        }, 2000)
    }
    if (length == 7) {
        rival.avatar = `<div style="position: fixed; top: 5%; right: 5%; width: 11vh; text-align: center">
        Flint
        <img style="width: 22vh; height: auto" src="https://archives.bulbagarden.net/media/upload/thumb/a/a8/Diamond_Pearl_Flint.png/150px-Diamond_Pearl_Flint.png" alt="">
        </div>
        `
        rival.name = 'Flint'
        battlemessage = 'Du slo Bertha!. Neste utfordrer er Flint'
        setTimeout(function () {
            rival.pokemon.splice(0, 1)
            battlemessage = rival.name + ' sendte ut ' + rival.pokemon[0].name + '!'
            pokemonsound = sounds[rival.pokemon[0].name.toLowerCase()];
            pokemonsound.play()
            resetstats('foe')
            updateview()
            setTimeout(function () {
                battlemessage = ''
                buttonsenabled = true
                updateview()
            }, 2000)
        }, 2000)
    }
    if (length == 5) {
        rival.avatar = `<div style="position: fixed; top: 5%; right: 5%; width: 11vh; text-align: center">
        Lucian
        <img style="width: 22vh; height: auto" src="pictures/lucian.png" alt="">
        </div>
        `
        rival.name = 'Lucian'
        battlemessage = 'Du slo Flint!. Neste utfordrer er Lucian'
        setTimeout(function () {
            rival.pokemon.splice(0, 1)
            battlemessage = rival.name + ' sendte ut ' + rival.pokemon[0].name + '!'
            pokemonsound = sounds[rival.pokemon[0].name.toLowerCase()];
            pokemonsound.play()
            resetstats('foe')
            updateview()
            setTimeout(function () {
                battlemessage = ''
                buttonsenabled = true
                updateview()
            }, 2000)
        }, 2000)
    }
    if (length == 3) {
        rival.avatar = `<div style="position: fixed; top: 5%; right: 5%; width: 11vh; text-align: center">
        Cynthia
        <img style="width: auto; height: 22vh" src="https://www.serebii.net/pokemonmasters/syncpairs/cynthia.png" alt="">
        </div>
        `
        elitefourtheme.pause()
        championtheme.play()
        championtheme.volume = 0.15
        rival.name = 'Cynthia'
        battlemessage = 'Du slo Lucian!. Neste utfordrer er Cynthia'
        setTimeout(function () {
            rival.pokemon.splice(0, 1)
            decidetheme()
            battlemessage = rival.name + ' sendte ut ' + rival.pokemon[0].name + '!'
            pokemonsound = sounds[rival.pokemon[0].name.toLowerCase()];
            pokemonsound.play()
            resetstats('foe')
            updateview()
            setTimeout(function () {
                battlemessage = ''
                buttonsenabled = true
                updateview()
            }, 2000)
        }, 2000)
    }
    if (length == 1) {
        battlemessage = 'Du slo Cynthia!. Du vant. Wooooooooow!'
        championtheme.pause()
        victorytheme.play()
        victorytheme.volume = 0.15
        buttonsenabled = false
    }
    buttonsenabled = true
    updateview()
}

function updateuattack(who) {
    if (who == 'friend') playerattack = uattackstate
    else rivalattack = uattackstate
}

function updateaattack(who) {
    if (who == 'friend') rivalattack = aattackstate
    else playerattack = aattackstate
}

function updateudefense(who) {
    if (who == 'friend') playerdefense = udefensestate
    else rivaldefense = udefensestate
}

function updateadefense(who) {
    if (who == 'friend') rivaldefense = adefensestate
    else playerdefense = udefensestate
}

function resetstats(who) {
    if (who == 'friend') {
        playerattack = 6
        playeraccuracy = 1
        playerdefense = 6
        playerstatus = false
        playerburned = false
        playerstatus = false
        playerpara = false
        playerstatusimage = ''

    }
    if (who == 'foe') {
        rivalattack = 6
        rivalaccuracy = 1
        rivaldefense = 6
        rivalstatus = false
        rivalburned = false
        rivalstatus = false
        rivalpara = false
        rivalstatusimage = ''
    }
}

function toggletheme() {
    if (rival.pokemon.length <= 2 && rival.pokemon[0].name != '') {
        if (!championtheme.paused) {
            championtheme.pause()
        } else {
            championtheme.play();
            championtheme.volume = 0.12
        }
    }
    if (rival.pokemon.length > 2 && currentview == "rival" || currentview == "changepokemon") {
        if (!elitefourtheme.paused) {
            elitefourtheme.pause()
        } else {
            elitefourtheme.play();
            elitefourtheme.volume = 0.12
        }
    }
    if (currentview != "rival" && currentview != "changepokemon") {
        if (!adventuretheme.paused) {
            adventuretheme.pause()
        } else {
            adventuretheme.play();
            adventuretheme.volume = 0.12
        }
    }
    if (rival.pokemon[0].name == '') {
        if (!victorytheme.paused) {
            victorytheme.pause()
        } else {
            victorytheme.play();
            victorytheme.volume = 0.12
        }
    }
}

function decidetheme() {
    if (currentview == 'caught' || currentview == 'randomencounter') document.getElementById('app').style.backgroundImage = "url('https://media-s3-us-east-1.ceros.com/hype-beast/images/2018/07/13/a9a51bc0b8d626db493ab5f9a971b043/background-hero.png')";
    if (currentview == 'start') document.getElementById('app').style.backgroundImage = "url('https://i.redd.it/bwnjsygz9q2b1.png')";
    if (currentview == 'rival') document.getElementById('app').style.backgroundImage = "url('https://wallpapercave.com/wp/wp11053878.jpg')";
    if (currentview == 'allcaught' || currentview == 'changepokemon') document.getElementById('app').style.backgroundImage = "url('https://cdnb.artstation.com/p/assets/images/images/037/494/913/large/unai-zabala-vergara-mochila-render.jpg?1620537472')";
    if (currentview == 'rival' && rival.pokemon.length <= 2) document.getElementById('app').style.backgroundImage = "url('https://wallpapers.com/images/hd/pokemon-stadium-1920-x-1080-wallpaper-dqqp686gjn57ka0u.jpg')";
}

function randomenemymove() {
    return Math.floor(Math.random() * 4)
}

function randommultimovenum() {
    return Math.ceil(Math.random() * 4) + 1
}

function randomaccuracy() {
    return Math.ceil(Math.random() * 100)
}

function whoismoving(who) {     // u = user og a = afflicted
    if (who == 'friend') {
        uname = player.pokemon[0].name
        aname = rival.pokemon[0].name
        ahp = rival.pokemon[0].health
        uhp = player.pokemon[0].health
        uattackstat = statstates[playerattack]
        uattackstate = playerattack
        udefensestat = statstates[playerdefense]
        udefensestate = playerdefense
        aattackstat = statstates[rivalattack]
        aattackstate = rivalattack
        adefensestat = statstates[rivaldefense]
        adefensestate = rivaldefense
        uburned = playerburned
        aburned = rivalburned
        upara = playerpara
        apara = rivalpara
        ustatus = playerstatus
        astatus = rivalstatus
        ustatusimage = playerstatusimage
        astatusimage = rivalstatusimage
        uaccuracy = playeraccuracy
        aaccuracy = rivalaccuracy
    } else {
        uname = rival.pokemon[0].name
        aname = player.pokemon[0].name
        uhp = rival.pokemon[0].health
        ahp = player.pokemon[0].health
        uattackstat = statstates[rivalattack]
        uattackstate = rivalattack
        udefensestat = statstates[rivaldefense]
        udefensestate = rivaldefense
        aattackstat = statstates[playerattack]
        aattackstate = playerattack
        adefensestat = statstates[playerdefense]
        adefensestate = playerdefense
        uburned = rivalburned
        aburned = playerburned
        upara = rivalpara
        apara = playerpara
        ustatus = rivalstatus
        astatus = playerstatus
        ustatusimage = rivalstatusimage
        astatusimage = playerstatusimage
        uaccuracy = rivalaccuracy
        aaccuracy = playeraccuracy
    }
}

function updateburn(who, newburn) {
    if (who == 'friend' && newburn == true) {
        rivalburned = newburn
        rivalstatus = newburn
        rivalstatusimage = burnimage
    }
    if (who == 'friend' && newburn == false) {
        playerstatus = newburn
        playerburned = newburn
        playerstatusimage = ''
    }

    if (who == 'foe' && newburn == true) {
        playerstatus = newburn
        playerburned = newburn
        playerstatusimage = burnimage
    }
    if (who == 'foe' && newburn == false) {
        rivalstatus = newburn
        rivalburned = newburn
        rivalstatusimage = ''
    }
}