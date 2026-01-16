


// https://www.hackerrank.com/challenges/one-month-preparation-kit-plus-minus/problem?isFullScreen=true&h_l=interview&playlist_slugs%5B%5D=preparation-kits&playlist_slugs%5B%5D=one-month-preparation-kit&playlist_slugs%5B%5D=one-month-week-one

function plusMinus(arr) {
    let l = arr.length
    console.log((arr.filter(e => e > 0).length/l).toPrecision(6))
    console.log((arr.filter(e => e < 0).length/l).toPrecision(6))
    console.log((arr.filter(e => e == 0).length/l).toPrecision(6))
}
