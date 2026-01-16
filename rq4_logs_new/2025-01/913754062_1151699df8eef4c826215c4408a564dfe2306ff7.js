const textlines = ["Hi there 👋", 
    "I'm Ron Mordukhovich", 
    "A Junior Developer"];

const headlineElement = document.getElementById('banner-headline');
const typingLine = document.getElementById('typing-line');
const cursor = document.getElementById('cursor');
let lineIndex = 0;
let charIndex = 0;

function writeText() {
    if (lineIndex < textlines.length) {
        if (charIndex < textlines[lineIndex].length) {
            cursor.insertAdjacentText('beforebegin', textlines[lineIndex].charAt(charIndex));
            charIndex++;
            setTimeout(writeText, 100);
        } else {
            if (lineIndex < textlines.length - 1) {
                const br = document.createElement('br');
                cursor.before(br);
            }
            charIndex = 0;
            lineIndex++;
            setTimeout(writeText, 500);
        }
    } else {
        headlineElement.removeChild(cursor);
    }
}

writeText();