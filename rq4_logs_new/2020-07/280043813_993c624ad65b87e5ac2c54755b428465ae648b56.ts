function bandScroll () {
    scroll = ["OPENING ACT", "FAMOUS BAND", "LOCAL PUNK BAND", "LOCAL METAL BAND", "OUT-OF-TOWN BAND", "LOCAL BURLESQUE SHOW", "PERFORMANCE ARTIST"]
    random = randint(0, 6)
    sendScroll()
}
function dailyScroll () {
    scroll = ["WELCOME TO THE PRE-APOCALYPSE!", "@realRevJesus", "Ask me about selling your soul!", "FSoFCabal.com", "I have seen the fnords", "1312", "15,517,801 CASES      632,914 DEAD      9,441,006 RECOVERED      4% MORTALITY RATE      0.2% WORLD POP AFFECTED      WEAR A FUCKING MASK", "r/SeattlePunk", "Don't panic - but wash your hands!", "I'm your Huckleberry", "GNU Terry Pratchett", "Nazi Punks Fuck Off", "OperationBrowniePockets.com", "The Army of the Twelve Monkeys was here", "+++       Out of Cheese Error       Redo From Start       Melon       Melon       Melon       +++", "Wubba-lubba dub dub!", "If you type 'google' into Google, it will break the Internet", "Earth: Mostly Harmless", "LEGALIZE MEDICAL COCAINE", "All that is gold does not glitter; not all those who wander are lost", "A man can never have too much red wine, too many books, or too much ammunition.", "We were somewhere around Barstow, on the edge of the desert, when the drugs began to take hold", "Graffiti is beautiful, like a brick in the face of a cop.", "Have you tried turning it off and back on again?", "OVERALL THERE IS A SMELL OF FRIED ONIONS", "I may be crazy, but that doesn't mean I'm wrong - I'm MAD, not ill!", "Currently drinking my way through the Coronapocalypse", "Think of how stupid the average person is - and realize half of them are stupider than that!", "Those who dance are considered insane by those who cannot hear the music", "They're probably foreigners, with ways different from our own - they may do some more...       folk dancing", "I am a leaf on the wind - watch how I soar", "I must not fear - fear is the mind-killer. Fear is the little death that brings total obliteration.", "Many that live deserve death, yet some that die deserve life - do not be too eager to deal out death in judgement.", "Welcome to Earth! It's hot in the summer and cold in the winter; it's round and wet and crowded.... there's only one rule I know of - 'Goddammit, you've got to be kind.'", "The enemy is anybody who's going to get you killed - no matter which side he's on.", "'TIS AN ILL WIND THAT BLOWS NO MINDS - for further information, consult your pineal gland", "Violence - naked force - has settled more issues in history than has any other factor, and the contrary opinion is wishful thinking at its worst.", "There is no overkill - there is only 'open fire' and 'reload.'", "War is an ill thing, as I well know - but 'twould be an ill world for weaponless dreamers, if evil men were not now and then slain.", "When did the future switch from being a promise to being a threat?", "KICK OUT THE JAMS", "WAR IS PEACE", "FREEDOM IS SLAVERY", "IGNORANCE IS STRENGTH"]
    random = randint(0, 40)
    sendScroll()
}
function arcaneBullshit () {
    basic.showLeds(`
        . . . . .
        . . # . .
        . . # # .
        . # # # .
        # # # # #
        `)
    scroll = ["LOOK AT THIS FUCKING HOURGLASS", "THE", "THE FROODUS", "THE PREGNANT BABIES", "THE GURU", "MAZE FACE", "HORSE DEATH", "THIEF", "SPACE DEATH", "THE PARASITIC FRUIT", "RHUBARB TONY", "SEX WITCH WITH OCTOPUS", "THE FORMER CHILD STAR", "THE DECISION MAKER", "THE PYRAMID-HEADED BIRD", "THE DOUBLE CAVEMAN", "THE ROBOT RIDING A GORILLA", "THE LORD OF THE HUNT", "THE PUPA VOMITING ON CATS", "KING FANCY", "THE WEREWOLF AND THE PIG MAKING OUT", "COFFIN FULL OF TACOS FULL OF SNAKES", "THE GRAVY FUNNEL", "MAKE UP YOUR GODDAMN MIND", "THE WEIRD TREE", "BASEBALL GOAT", "THE DILDO WIZARD", "HALF-PRICE HOT WINGS", "NIP-SLIP POPE", "THE ALL-SEEING SLICE", "KNEES", "NOBODY", "SOMETHING", "INSTINCT", "THE BEEFCAKE", "SAXOPHONE FETUS", "PIT BULL DRIVING A TINY TRUCK", "CARD OF CARDS", "THE SWORD HOARDER", "THE GOLDEN RETRIEVER", "THE GREASED PRIEST", "RANDOM CLIP ART", "FUTURE GRANDMA", "EVERYTHING'S FINE", "RAPTOR RIDING A DOLPHIN IN SPACE", "ROBODODO", "THE ANTHROPOMORPHIC CARROT", "CHAOS MOM", "IT'S PROBABLY NOTHING", "YOU'RE FUCKED", "SHRIMP AND GUNS", "THE INVERTED SQUIRREL", "THE SURFIN' CHIMP", "SHARK, TROMBONE, AND THREE SANDWICHES", "FUTURE NOT FOUND", "THE BIRD-BOOK-SNAKE THING", "THE RAT TROUT", "THE PANTSLESS BEEKEPER", "I HAVE NO FUCKING CLUE", "THE FALSE FACE", "GIVING BIRTH TO A SKULL", "THE QUEEN OF HOLES", "WOLVES", "CRAZY MYSTICAL SHIT", "INFINITE COSMIC MESS", "THE FLYING MAN", "THE FILTHY MATRESS", "THE FLOPPY KEY", "GALAXY DAD", "THE GIANT SPERM FIGHTING A DOVE", "THE SKELETON EATING A BANANA", "THE PIE KNIGHT", "THE HERON BEARER", "MEANINGLESS GARBAGE", "URINAL DEATH", "THE ORGY", "OLD MAN POURING WATER ON AN EYEBALL", "THE GROSS CUP", "MADAME RUFFLEBONNET", "THE FERAL NERD", "THE PENIS WEASEL", "THE SHITTY ACOUSTIC GUITAR GUY", "THE DIGGING GHOST", "THE EAGLE CHAINED TO A CALF", "ANNIHILATION", "BIRTHDAY CORPSE", "DEATH DEATH", "PAPA MACHO", "A WOMAN BREASTFEEDING A VCR", "A CAT LICKING ITS BUTT FOR ETERNITY", "A BUNCH OF CATS POSING AS A WOMAN", "BIG SEXY SATAN", "THIS CARD IS DEFINITELY ABOUT SEX", "LIL' PABLO", "THIS FUCKING GUY", "YOU'RE SO FUCKING FUCKED", "HANG IN THERE", "AUNT BRENDA", "VENGEANCE", "THE PROVIDER", "THE SUPER HORNY PORNO OWL", "WET PANTS", "KING SHIT", "THE TOILET ANGEL", "A CACTUS ON A MOTORCYCLE WITH UDDERS", "A UTERUS WITH FEET"]
    random = randint(0, 105)
    reverse = Math.randomBoolean()
    rune = Math.randomBoolean()
    if (rune == true) {
        futhark()
    }
    sendScroll()
}
function futhark () {
    random = randint(0, 24)
    if (random == 0) {
        basic.showLeds(`
            . . . . .
            . . . . .
            . . . . .
            . . . . .
            . . . . .
            `)
    } else if (random == 1) {
        basic.showLeds(`
            . # . # .
            . # # . .
            . # . # .
            . # # . .
            . # . . .
            `)
    } else if (random == 2) {
        basic.showLeds(`
            . # . . .
            . # # . .
            . # . # .
            . # . # .
            . # . # .
            `)
    } else if (random == 3) {
        basic.showLeds(`
            . # . . .
            . # # . .
            . # . # .
            . # # . .
            . # . . .
            `)
    } else if (random == 4) {
        basic.showLeds(`
            . # # . .
            . # . # .
            . # . . .
            . # # . .
            . # . # .
            `)
    } else if (random == 5) {
        basic.showLeds(`
            . # . . .
            . # # . .
            . # . # .
            . # # . .
            . # . # .
            `)
    } else if (random == 6) {
        basic.showLeds(`
            . . . # .
            . . # . .
            . # . . .
            . . # . .
            . . . # .
            `)
    } else if (random == 7) {
        basic.showLeds(`
            # . . . #
            . # . # .
            . . # . .
            . # . # .
            # . . . #
            `)
    } else if (random == 8) {
        basic.showLeds(`
            . # # . .
            . # . # .
            . # # . .
            . # . . .
            . # . . .
            `)
    } else if (random == 9) {
        basic.showLeds(`
            . # . . #
            . # . . #
            . # # . #
            . # . # #
            . # . . #
            `)
    } else if (random == 10) {
        basic.showLeds(`
            # . # . .
            . # # . .
            . . # # .
            . . # . #
            . . # . .
            `)
    } else if (random == 11) {
        basic.showLeds(`
            . . # . .
            . . # . .
            . . # . .
            . . # . .
            . . # . .
            `)
    } else if (random == 12) {
        basic.showLeds(`
            . # . . .
            # . . . .
            . # . # .
            . . . . #
            . . . # .
            `)
    } else if (random == 13) {
        basic.showLeds(`
            . . # # .
            . . # . #
            . . # . .
            # . # . .
            . # # . .
            `)
    } else if (random == 14) {
        basic.showLeds(`
            # . # . #
            . # # # .
            . . # . .
            . . # . .
            . . # . .
            `)
    } else if (random == 15) {
        basic.showLeds(`
            # # . # .
            # . # . .
            # . . . .
            # . # . .
            # # . # .
            `)
    } else if (random == 16) {
        basic.showLeds(`
            # . . . .
            # . . # .
            # . # # .
            # # . # .
            . . . # .
            `)
    } else if (random == 17) {
        basic.showLeds(`
            . . # . .
            . # # # .
            # . # . #
            . . # . .
            . . # . .
            `)
    } else if (random == 18) {
        basic.showLeds(`
            . # # . .
            . # . # .
            . # # . .
            . # . # .
            . # # . .
            `)
    } else if (random == 19) {
        basic.showLeds(`
            # # . # #
            # . # . #
            # . . . #
            # . . . #
            # . . . #
            `)
    } else if (random == 20) {
        basic.showLeds(`
            # # . # #
            # . # . #
            # # . # #
            # . . . #
            # . . . #
            `)
    } else if (random == 21) {
        basic.showLeds(`
            . # . . .
            . # # . .
            . # . # .
            . # . . .
            . # . . .
            `)
    } else if (random == 22) {
        basic.showLeds(`
            # # . # #
            . . # . .
            # # . # #
            . . # . .
            # # . # #
            `)
    } else if (random == 23) {
        basic.showLeds(`
            . . . . .
            . . # . .
            . # . # .
            . . # . .
            . # . # .
            `)
    } else if (random == 24) {
        basic.showLeds(`
            # . . . #
            # # . # #
            # . # . #
            # # . # #
            # . . . #
            `)
    }
}
input.onButtonPressed(Button.A, function () {
    _8ball()
})
function modeChange () {
    reverse = Math.randomBoolean()
    random = 0
    altscr = false
    if (concertMode == false) {
        basic.showLeds(`
            # . . # .
            # . . # .
            # # # # #
            # # # # .
            . # # . .
            `)
        concertMode = true
        scroll = ["SHOW MODE"]
        sendScroll()
        basic.clearScreen()
    } else if (concertMode == true) {
        basic.showIcon(IconNames.StickFigure)
        concertMode = false
        scroll = ["DAILY DRIVER"]
        sendScroll()
        basic.clearScreen()
    }
}
input.onButtonPressed(Button.AB, function () {
    modeChange()
})
input.onButtonPressed(Button.B, function () {
    arcaneBullshit()
})
function seattleScroll () {
    altScroll = ["EIGHT MINUTES AND FORTY-SIX SECONDS", "SAVE OUR LOCAL VENUES", "SUPPORT LOCAL MUSIC", "SUPPORT SMALL BUSINESS", "BUY LOCAL WEED -- STARVE THE CARTELS", "DRINK LOCAL - SUPPORT OUR BARS", "TO THE POLLS YE SONS OF FREEDOM", "TAKE A FRIEND TO VOTE - ELECTIONS HAVE CONSEQUENCES!"]
    random = randint(0, 7)
    sendScroll()
}
function sendScroll () {
    if (reverse == true) {
        scrollbit.setUpsideDown(true)
    } else {
        scrollbit.setUpsideDown(false)
    }
    if (altscr == true) {
        scrollbit.scrollText(altScroll[random], randint(200, 255), randint(0, Math.abs(input.temperature())))
    } else {
        scrollbit.scrollText(scroll[random], randint(200, 255), randint(0, Math.abs(input.temperature())))
    }
}
function heartBeats () {
    beats = randint(0, 99)
    basic.pause(2 * randint(input.temperature(), 5 * input.temperature()))
    basic.showIcon(IconNames.SmallHeart)
    control.waitMicros(1000 * randint(input.temperature(), 5 * input.temperature()))
    reverse = Math.randomBoolean()
    if (beats == 13) {
        basic.showIcon(IconNames.Skull)
        altscr = true
    } else if (beats == input.temperature()) {
        basic.showIcon(IconNames.Duck)
        altscr = true
    } else if (beats == 66) {
        basic.showIcon(IconNames.Sword)
        altscr = true
    } else if (beats == 42) {
        basic.showIcon(IconNames.EigthNote)
        altscr = true
    } else if (beats == 99) {
        for (let index = 0; index < randint(Math.round(input.temperature() / 2), input.lightLevel() / 5); index++) {
            basic.showIcon(IconNames.SmallHeart)
            basic.showIcon(IconNames.Heart)
        }
        altscr = true
    } else {
        if (concertMode == false) {
            basic.showIcon(IconNames.Heart)
        } else if (concertMode == true) {
            basic.showLeds(`
                # . . # .
                # . . # .
                # # # # #
                # # # # .
                . # # . .
                `)
        }
        reverse = false
        altscr = false
        doScroll = Math.randomBoolean()
    }
    basic.clearScreen()
    if (concertMode == true && altscr == true) {
        altscr = Math.randomBoolean()
        reverse = Math.randomBoolean()
        if (altscr == true) {
            seattleScroll()
        } else {
            dailyScroll()
        }
    } else if (concertMode == true && altscr == false) {
        bandScroll()
        reverse = Math.randomBoolean()
    } else if (concertMode == false && altscr == true) {
        reverse = Math.randomBoolean()
        seattleScroll()
    } else if (concertMode == false && altscr == false) {
        reverse = false
        dailyScroll()
    }
    control.waitMicros(10000 * randint(5 * input.temperature(), 10 * input.temperature()))
}
function _8ball () {
    basic.showLeds(`
        . . . . .
        . . # . .
        . . . . .
        . . # . .
        . . . . .
        `)
    basic.showLeds(`
        . . # . .
        . # . # .
        . . # . .
        . # . # .
        . . # . .
        `)
    basic.showLeds(`
        . # # # .
        . # . # .
        . # # # .
        . # . # .
        . # # # .
        `)
    random = randint(0, 2)
    if (random == 0) {
        scroll = ["Very doubtful", "You've got to be kidding", "Outlook not so good", "Yeah, and I'm the Pope", "My sources say no", "Not a fuckin' chance", "In your dreams", "Yeah, right", "Not on your life", "You wish", "My reply is no", "Don't count on it", "Not in a million years", "Not gonna happen"]
        random = randint(0, 13)
    } else if (random == 1) {
        scroll = ["As I see it, yes", "It is certain", "It is decidedly so", "Most likely", "Outlook good", "Signs point to yes", "Without a doubt", "Yes", "Yes - definitely", "You may rely on it", "Yes, but do it drunk as fuck", "Yes, if you leave me alone"]
        random = randint(0, 11)
    } else if (random == 2) {
        scroll = ["Get a clue", "Ask again later", "Who cares", "Better not tell you now", "Oh, please", "Cannot predict now", "Concentrate and ask again", "Reply hazy, try again", "Definitely perhaps", "Battery low; try again later", "Don't ask me, I'm just a circuitboard", "My sources say no, but they also said Hillary would win"]
        random = randint(0, 11)
    }
    sendScroll()
    basic.clearScreen()
}
let doScroll = false
let beats = 0
let altScroll: string[] = []
let altscr = false
let rune = false
let reverse = false
let random = 0
let scroll: string[] = []
let concertMode = false
concertMode = true
modeChange()
basic.forever(function () {
    heartBeats()
})