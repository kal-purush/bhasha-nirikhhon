(function() {
    $('#laptop').keydown(function(e) {
        var $this, code, end, start, value;
        code = e.keyCode || e.which;
        //console.log code
        if (code === 18 || code === 16) {
            $('#kc-' + code + 'R').addClass('active');
            $('#kc-' + code + 'L').addClass('active');
            
        } else {
            $('#kc-' + code).addClass('active');
            console.log(code);
        }
        // spaces on tab
        if (code === 9) {
            start = this.selectionStart;
            end = this.selectionEnd;
            $this = $(this);
            value = $this.val();
            $this.val(value.substring(0, start) + "  " + value.substring(end));
            this.selectionStart = this.selectionEnd = start + 2;
            return e.preventDefault();
        }
    });
    $('#laptop').keyup(function(e) {
        var code;
        code = e.keyCode || e.which;
        if (code === 18 || code === 16) {
            $('#kc-' + code + 'R').removeClass('active');
            return $('#kc-' + code + 'L').removeClass('active');
        } else {
            $('#kc-' + code).css('background-color', 'rgb(97, 97, 97)');
            return $('#kc-' + code).removeClass('active');
        }
    });



}).call(this);

function init() {

    wait(1000).then(() => {
        clearText()
        typeText('De volgende stap in, ').then(typeLoop)
    })

    function typeLoop() {
        typeText('Typetrainingen!')
            .then(() => wait(2000))
            .then(() => removeText('Typetrainingen!'))
            .then(() => typeText('Typevaardigheid!'))
            .then(() => wait(2000))
            .then(() => removeText('Typevaardigheid!'))
            .then(() => typeText('Typecurssusen!'))
            .then(() => wait(2000))
            .then(() => removeText('De volgende stap in, Typecurssusen!'))
            .then(() => typeText('De volgende stap in, '))
            .then(typeLoop)
    }

}

// Source code 🚩

const elementNode = document.getElementById('type-text')
let text = ''

function updateNode() {
    elementNode.textContent = text
}

function pushCharacter(character) {
    text += character
    updateNode()
}

function popCharacter() {
    text = text.slice(0, text.length - 1)
    updateNode()
}

function clearText() {
    text = ''
    updateNode()
}


function wait(time) {
    if (time === undefined) {
        const randomMsInterval = 100 * Math.random()
        time = randomMsInterval < 50 ? 10 : randomMsInterval
    }

    return new Promise(resolve => {
        setTimeout(() => {
            requestAnimationFrame(resolve)
        }, time)
    })
}

function typeCharacter(character) {
    return new Promise(resolve => {
        pushCharacter(character)
        wait().then(resolve)
    })
}

function removeCharacter() {
    return new Promise(resolve => {
        popCharacter()
        wait().then(resolve)
    })
}

function typeText(text) {
    return new Promise(resolve => {

        function type([character, ...text]) {
            typeCharacter(character)
                .then(() => {
                    if (text.length) type(text)
                    else resolve()
                })
        }

        type(text)
    })
}

function removeText({ length: amount }) {
    return new Promise(resolve => {

        function remove(count) {
            removeCharacter()
                .then(() => {
                    if (count > 1) remove(count - 1)
                    else resolve()
                })
        }

        remove(amount)
    })
}


init()

function loadBackground() {
    var keys = "ABCDEFGH".split("");
    keys.forEach(fillContainerWithKeys);
    keys = "STUVWXYZ".split("");
    keys.forEach(fillBigContainerWithKeys);
    keys = "$%&/;@!?".split("");
    keys.forEach(fillSmallContainerWithKeys);
}

function fillContainerWithKeys(key, index) {
    $('<div class="key" type="button"><span class="text">' + key + '</span><div class="angle-shadow left-top top-section"></div> <div class="angle-shadow right-top top-section"></div><div class="angle-shadow left-bottom bottom-section"></div><div class="angle-shadow right-bottom bottom-section"></div></div>').appendTo("#container-background-keys");
}

function fillBigContainerWithKeys(key, index) {
    $('<div class="key" type="button"><span class="text">' + key + '</span><div class="angle-shadow left-top top-section"></div> <div class="angle-shadow right-top top-section"></div><div class="angle-shadow left-bottom bottom-section"></div><div class="angle-shadow right-bottom bottom-section"></div></div>').appendTo("#container-background-big-keys");
}

function fillSmallContainerWithKeys(key, index) {
    $('<div class="key" type="button"><span class="text">' + key + '</span><div class="angle-shadow left-top top-section"></div> <div class="angle-shadow right-top top-section"></div><div class="angle-shadow left-bottom bottom-section"></div><div class="angle-shadow right-bottom bottom-section"></div></div>').appendTo("#container-background-small-keys");
}
loadBackground();

p1Button.onclick = function() {

    setTimeout(function() {
        document.getElementById('firstScreen').style.opacity = '0.1';
        document.getElementById('loadingAnimation').style.display = 'block';

    }, 300);
    setTimeout(function() {
        document.getElementById('loadingAnimation').style.display = "none";
        document.getElementById('firstScreen').style.display = 'none';
        document.getElementById('secondScreen').style.display = 'grid';
        document.getElementById('secondScreen').style.opacity = '1';
    }, 2000);
}
p2Button.onclick = function() {

    setTimeout(function() {
        document.getElementById('secondScreen').style.opacity = '1';
        setTimeout(function() {
            document.getElementById('secondScreen').style.opacity = '0.1';
            document.getElementById('loadingAnimation').style.display = 'block';
            setTimeout(function() {
                document.getElementById('loadingAnimation').style.display = "none";
                document.getElementById('secondScreen').style.display = 'none';
                document.getElementById('thirdScreen').style.display = 'grid';
                document.getElementById('thirdScreen').style.opacity = '1';
            }, 2000);
        }, 2000);


    }, 4000);


}

backP1Button.onclick = function() {

    setTimeout(function() {
        document.getElementById('secondScreen').style.opacity = '0.1';
        document.getElementById('loadingAnimation').style.display = 'block';
    }, 400);
    setTimeout(function() {
        document.getElementById('loadingAnimation').style.display = "none";
        document.getElementById('firstScreen').style.display = 'grid';
        document.getElementById('secondScreen').style.display = 'none';
        document.getElementById('firstScreen').style.opacity = '1';
    }, 2000);
}

backP2Button.onclick = function() {

    setTimeout(function() {
        document.getElementById('thirdScreen').style.opacity = '0.1';
        document.getElementById('loadingAnimation').style.display = 'block';
    }, 400);
    setTimeout(function() {
        document.getElementById('loadingAnimation').style.display = "none";
        document.getElementById('secondScreen').style.display = 'grid';
        document.getElementById('thirdScreen').style.display = 'none';
        document.getElementById('secondScreen').style.opacity = '1';
    }, 2000);
}

codeNo.onclick = function() {
    document.getElementById('codeInput').style.display = 'none';
    document.getElementById('codeInput').style.alignSelf = 'center';
}


codeYes.onclick = function() {
    document.getElementById('codeInput').style.display = 'grid';
}
extraPersone.onclick = function() {
    document.getElementById('phone').style.display = 'grid';
}

phoneClose.onclick = function() {
    document.getElementById('phone').style.display = 'none';
    document.getElementById("phoneInput").clearText;
}


OnePayment.onclick = function() {
    document.getElementById('directPay').style.display = 'grid';
    document.getElementById('overTimePay').style.display = 'none';
    document.getElementById('termsSevice').style.display = 'none';
    setTimeout(function() {
        document.getElementById('termsSevice').style.display = 'grid';
    }, 1);
}
FourPayments.onclick = function() {
    document.getElementById('directPay').style.display = 'none';
    document.getElementById('overTimePay').style.display = 'grid';
    document.getElementById('termsSevice').style.display = 'none';
    setTimeout(function() {
        document.getElementById('termsSevice').style.display = 'grid';
    }, 1);
}
TwelfePayments.onclick = function() {
    document.getElementById('directPay').style.display = 'none';
    document.getElementById('overTimePay').style.display = 'grid';
    document.getElementById('termsSevice').style.display = 'none';
    setTimeout(function() {
        document.getElementById('termsSevice').style.display = 'grid';
    }, 1);
}