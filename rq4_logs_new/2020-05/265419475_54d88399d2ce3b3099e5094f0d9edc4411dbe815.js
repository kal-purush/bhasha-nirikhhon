$('#Squad-btn').click(function() {
    location.href = '#topItems';
});

//Squadron Redirects

//Simon
function SimonInsta() {
    window.open('https://www.instagram.com/simonv54/');
}
function SimonYoutube() {
    window.open('https://www.youtube.com/channel/UCUwtVLP6AyIEHFcIjC9VPtQ?view_as=subscriber');
}
function SimonTwitch() {
    window.open('https://www.twitch.tv/theonlysova');
}

//Donovan
function DonInsta1() {
    window.open('https://www.instagram.com/donovan_staab/');
}
function DonYoutube() {
    window.open('https://www.youtube.com/channel/UCc-0PypcQVMUi3EL_zesGtg?view_as=subscriber');
}
function DonInsta2() {
    window.open('https://www.instagram.com/donovan_photo/');
}

//Liam
function LiamInsta() {
    window.open('https://www.instagram.com/liham825/');
}
function LiamYoutube() {
    window.open('https://www.youtube.com/channel/UCL2DPRFRT0BRdHbBaOHXUcw?view_as=subscriber');
}
function LiamTwitch() {
    window.open('https://www.twitch.tv/liam825');
}

//Bean
function BeanInsta() {
    window.open('https://www.instagram.com/luke_keller15/?hl=en');
}
function BeanSnap() {
    window.open('https://www.snapchat.com/add/keller_luke');
}
function BeanTwitter() {
    window.open('https://twitter.com/lukekeller15');
}

//Devin
function DevinInsta() {
    window.open('https://www.instagram.com/devinharris.36/?hl=en');
}
function DevinYoutube() {
    window.open('https://www.youtube.com/channel/UCAiBPt7UNw6jYo7RlIYnNcg?view_as=subscriber');
}
function DevinWeb() {
    window.open('https://devinharris.dev/');
}


//Follow Page redirects
function DiscLink() {
    window.open('https://discord.gg/mwWNTs2');
}

//Hamburger Click animation setup

let deviceWidth = screen.width;
let upper = document.getElementsByClassName('T');
let middle = document.getElementsByClassName('M');
let bottom = document.getElementsByClassName('B');
let hamDiv = document.querySelector('#hamburger');
let linkDiv = document.querySelector('#links');
let Ulist = document.querySelector('#links ul');
let nav = document.querySelector('nav');
let html = document.querySelector('html');
let LineTraverseHeight = $(middle).outerHeight(true) + 'px';
console.log(LineTraverseHeight);

let links = document.querySelectorAll('#links ul li a');
let linksArray = [];

for (let i = 0; i < links.length; ++i) {
    linksArray.push(links[i]);
}

let tl = new TimelineLite({ paused: true, reversed: true });
let t2 = new TimelineLite({ paused: true, reversed: true });

tl
    .to(html, 0, { overflowY: 'hidden' }, 'start')
    .to(upper, 0.5, { rotate: '45deg', x: 1, y: LineTraverseHeight, ease: Power2.easeInOut }, 'start')
    .to(middle, 0.5, { autoAlpha: 0 }, 'start')
    .to(bottom, 0.5, { rotate: '-45deg', x: 1, y: '-' + LineTraverseHeight, ease: Power2.easeInOut }, 'start');

t2
    .add(function () {
        $(linkDiv).css({
            padding: '15% 3%',
            position: 'absolute',
            top: '5rem',
            right: 0,
            width: '100vw',
            height: 'max-content',
            border: '3px solid white',
        }).slideToggle();

    });

//Once Ham is clicked trigger animation
$('#hamburger').click(function () {
    if (deviceWidth < 1100) {
        tl.reversed() ? tl.play() : tl.reverse();
        t2.reversed() ? t2.play() : t2.reverse();
    }
});

//Once Link in ham is clicked trigger animation
$('#links ul li a').click(function () {
    if (deviceWidth < 1100) {
        tl.reversed() ? tl.play() : tl.reverse();
        t2.reversed() ? t2.play() : t2.reverse();
    }
});