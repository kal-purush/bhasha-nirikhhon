export default function numStrMulty100(numStr) {
const arr = numStr.split('.');
if (arr.length === 1) return +(arr[0]+'00');
return +(arr.map((s, i) => (i === 1 && s.length === 1 ? s + "0" : s)).join(""));
}