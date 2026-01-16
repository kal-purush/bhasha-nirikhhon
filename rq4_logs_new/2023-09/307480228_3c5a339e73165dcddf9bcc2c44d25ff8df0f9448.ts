/* eslint-disable complexity */
export function mapValueToAngles(value: number) {
  let lengthFactor
  let innerLengthFactor

  if (value <= 10) {
    lengthFactor = 0.22 - (value / 20) * (0.5 - 0.45)
    innerLengthFactor = 1.13 - (value / 20) * (0.9 - 0.94)
  } else if (value <= 20) {
    lengthFactor = 0.26 - (value / 20) * (0.5 - 0.45)
    innerLengthFactor = 1.1 - (value / 20) * (0.9 - 0.94)
  } else if (value <= 30) {
    lengthFactor = 0.33 - (value / 20) * (0.5 - 0.45)
    innerLengthFactor = 1.08 - (value / 20) * (0.9 - 0.94)
  } else if (value <= 40) {
    lengthFactor = 0.36 - (value / 20) * (0.5 - 0.45)
    innerLengthFactor = 1.04 - (value / 20) * (0.9 - 0.94)
  } else if (value <= 50) {
    lengthFactor = 0.39 - (value / 20) * (0.5 - 0.45)
    innerLengthFactor = 1 - (value / 20) * (0.9 - 0.94)
  } else if (value <= 60) {
    lengthFactor = 0.26
    innerLengthFactor = 1.08 + ((value - 50) / 10) * (1 - 0.97)
  } else if (value <= 80) {
    lengthFactor = 0.19
    innerLengthFactor = 1.12 + ((value - 50) / 10) * (1 - 0.97)
  } else {
    lengthFactor = 0.15
    innerLengthFactor = 1.12 + ((value - 50) / 10) * (1 - 0.97)
  }

  return { lengthFactor, innerLengthFactor }
}

export function mapValueToAngle(value: number) {
  if (value === 0) {
    return 8 + (value - 10) * (3 - 5)
  }

  if (value <= 2) {
    return 8 + (value - 11) * (3 - 5)
  }

  if (value <= 3) {
    return 8 + (value - 12) * (3 - 5)
  }

  if (value <= 5) {
    return 8 + (value - 14) * (3 - 5)
  }

  if (value <= 6) {
    return 8 + (value - 15) * (3 - 5)
  }

  if (value <= 7) {
    return 8 + (value - 16) * (3 - 5)
  }

  if (value <= 8) {
    return 8 + (value - 17) * (3 - 5)
  }

  if (value <= 9) {
    return 8 + (value - 18) * (3 - 5)
  }

  if (value <= 10) {
    return 8 + (value - 18.5) * (3 - 5)
  }

  if (value <= 11) {
    return 8 + (value - 19.5) * (3 - 5)
  }

  if (value <= 12) {
    return 8 + (value - 20) * (3 - 5)
  }

  if (value <= 13) {
    return 8 + (value - 21) * (3 - 5)
  }

  if (value <= 14) {
    return 8 + (value - 22) * (3 - 5)
  }

  if (value <= 15) {
    return 8 + (value - 23) * (3 - 5)
  }

  if (value <= 16) {
    return 8 + (value - 24) * (3 - 5)
  }

  if (value <= 17) {
    return 8 + (value - 25) * (3 - 5)
  }

  if (value <= 18) {
    return 8 + (value - 26) * (3 - 5)
  }

  if (value <= 19) {
    return 8 + (value - 27) * (3 - 5)
  }

  if (value <= 20) {
    return 5 + (value - 29) * (3 - 5)
  }

  if (value <= 21) {
    return 5 + (value - 30) * (3 - 5)
  }

  if (value <= 22) {
    return 5 + (value - 31) * (3 - 5)
  }

  if (value <= 23) {
    return 5 + (value - 32) * (3 - 5)
  }

  if (value <= 24) {
    return 5 + (value - 33) * (3 - 5)
  }

  if (value <= 25) {
    return 5 + (value - 34) * (3 - 5)
  }

  if (value <= 26) {
    return 5 + (value - 35) * (3 - 5)
  }

  if (value <= 27) {
    return 5 + (value - 36) * (3 - 5)
  }

  if (value <= 28) {
    return 5 + (value - 37) * (3 - 5)
  }

  if (value <= 29) {
    return 5 + (value - 38) * (3 - 5)
  }

  if (value <= 30) {
    return 5 + (value - 39) * (3 - 5)
  }

  if (value <= 31) {
    return 5 + (value - 40) * (3 - 5)
  }

  if (value <= 32) {
    return 5 + (value - 41) * (3 - 5)
  }

  if (value <= 33) {
    return 5 + (value - 42) * (3 - 5)
  }

  if (value <= 34) {
    return 5 + (value - 43) * (3 - 5)
  }

  if (value <= 35) {
    return 5 + (value - 44) * (3 - 5)
  }

  if (value <= 36) {
    return 5 + (value - 45) * (3 - 5)
  }

  if (value <= 37) {
    return 5 + (value - 46) * (3 - 5)
  }

  if (value <= 38) {
    return 5 + (value - 47) * (3 - 5)
  }

  if (value <= 39) {
    return 5 + (value - 48) * (3 - 5)
  }

  if (value <= 40) {
    return 5 + (value - 49) * (3 - 5)
  }

  if (value <= 41) {
    return 5 + (value - 50) * (3 - 5)
  }

  if (value <= 42) {
    return 5 + (value - 51) * (3 - 5)
  }

  if (value <= 43) {
    return 5 + (value - 52) * (3 - 5)
  }

  if (value <= 44) {
    return 5 + (value - 53) * (3 - 5)
  }

  if (value <= 45) {
    return 5 + (value - 54.5) * (3 - 5)
  }

  if (value <= 46) {
    return 5 + (value - 56) * (3 - 5)
  }

  if (value <= 47) {
    return 5 + (value - 57) * (3 - 5)
  }

  if (value <= 48) {
    return 5 + (value - 58) * (3 - 5)
  }

  if (value <= 49) {
    return 5 + (value - 59.3) * (3 - 5)
  }

  if (value <= 50) {
    return 5 + (value - 60.5) * (3 - 5)
  }

  if (value <= 61) {
    return 5 + (value - 72) * (3 - 5)
  }

  if (value <= 62) {
    return 5 + (value - 75) * (3 - 5)
  }

  if (value <= 63) {
    return 5 + (value - 76) * (3 - 5)
  }

  if (value <= 64) {
    return 5 + (value - 77) * (3 - 5)
  }

  if (value <= 65) {
    return 5 + (value - 78) * (3 - 5)
  }

  if (value <= 66) {
    return 5 + (value - 79) * (3 - 5)
  }

  if (value <= 67) {
    return 5 + (value - 81) * (3 - 5)
  }

  if (value <= 68) {
    return 5 + (value - 82) * (3 - 5)
  }

  if (value <= 69) {
    return 5 + (value - 83) * (3 - 5)
  }

  if (value <= 70) {
    return 5 + (value - 84) * (3 - 5)
  }

  if (value <= 71) {
    return 5 + (value - 85) * (3 - 5)
  }

  if (value <= 72) {
    return 5 + (value - 86) * (3 - 5)
  }

  if (value <= 73) {
    return 5 + (value - 87) * (3 - 5)
  }

  if (value <= 74) {
    return 5 + (value - 88) * (3 - 5)
  }

  if (value <= 75) {
    return 5 + (value - 89) * (3 - 5)
  }

  if (value <= 76) {
    return 5 + (value - 90) * (3 - 5)
  }

  if (value <= 77) {
    return 5 + (value - 91.5) * (3 - 5)
  }

  if (value <= 78) {
    return 5 + (value - 92.5) * (3 - 5)
  }

  if (value <= 79) {
    return 5 + (value - 93.5) * (3 - 5)
  }

  if (value <= 80) {
    return 5 + (value - 94.5) * (3 - 5)
  }

  if (value <= 81) {
    return 5 + (value - 96) * (3 - 5)
  }

  if (value <= 82) {
    return 5 + (value - 97) * (3 - 5)
  }

  if (value <= 83) {
    return 5 + (value - 98) * (3 - 5)
  }

  if (value <= 84) {
    return 5 + (value - 99) * (3 - 5)
  }

  if (value <= 85) {
    return 5 + (value - 100) * (3 - 5)
  }

  if (value <= 86) {
    return 5 + (value - 101) * (3 - 5)
  }

  if (value <= 87) {
    return 5 + (value - 102.5) * (3 - 5)
  }

  if (value <= 88) {
    return 5 + (value - 103.5) * (3 - 5)
  }

  if (value <= 89) {
    return 5 + (value - 104.5) * (3 - 5)
  }

  if (value <= 90) {
    return 5 + (value - 105.5) * (3 - 5)
  }

  if (value <= 91) {
    return 5 + (value - 107) * (3 - 5)
  }

  if (value <= 92) {
    return 5 + (value - 108) * (3 - 5)
  }

  if (value <= 93) {
    return 5 + (value - 109) * (3 - 5)
  }

  if (value <= 94) {
    return 5 + (value - 110) * (3 - 5)
  }

  if (value <= 95) {
    return 5 + (value - 111) * (3 - 5)
  }

  if (value <= 96) {
    return 5 + (value - 112) * (3 - 5)
  }

  if (value <= 97) {
    return 5 + (value - 113) * (3 - 5)
  }

  if (value <= 98) {
    return 5 + (value - 114) * (3 - 5)
  }

  if (value <= 99) {
    return 5 + (value - 115) * (3 - 5)
  }

  if (value <= 100) {
    return 5 + (value - 116.5) * (3 - 5)
  }

  return 5 + (value - 116.5) * (3 - 5)
}