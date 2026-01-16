const fs = require('fs');
const inputArray = fs.readFileSync('input.txt').toString().split("\n");

const octopi = [];
for (const line of inputArray) {
  octopi.push(line.split("").map(Number));
}

const height = octopi.length;
const width = octopi[0].length;

// Part One
// const runs = 100;

// Part Two
const runs = 1000;

let flashes = 0;

function flash(flashed) {
  let flashing = 0;
  for (let h = 0; h < height; h++) {
    for (let w = 0; w < width; w++) {
      const coords = `${h}-${w}`;
      if (!flashed.includes(coords) && octopi[h][w] > 9) {
        flashes++;
        flashing++;
        flashed.push(coords);
        if (h >= 1) {
          if (w >= 1) octopi[h-1][w-1]++;
          octopi[h-1][w]++;
          if (w < width - 1) octopi[h-1][w+1]++;
        }
        if (w >= 1) octopi[h][w-1]++;
        if (w < width - 1) octopi[h][w+1]++;
        if (h < height - 1) {
          if (w >= 1) octopi[h+1][w-1]++;
          octopi[h+1][w]++;
          if (w < width - 1) octopi[h+1][w+1]++;
        }

      }
    }
  }
  if (flashing === 0) return;
  flash(flashed);
  return;
}

for (let r = 1; r <= runs; r++) {
  const flashed = [];
  for (let h = 0; h < height; h++) {
    for (let w = 0; w < width; w++) {
      octopi[h][w]++;
    }
  }
  flash(flashed);
  for (let h = 0; h < height; h++) {
    for (let w = 0; w < width; w++) {
      if (octopi[h][w] > 9) {
        octopi[h][w] = 0;
      }
    }
  }

  if (octopi.flat().reduce((a, b) => parseInt(a) + parseInt(b), 0) === 0) {
    console.log(`Synchronized on run ${r}`);
    break;
  }
}

console.log(`Total flashes: ${flashes}`);