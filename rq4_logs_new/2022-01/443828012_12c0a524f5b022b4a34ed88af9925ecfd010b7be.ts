import { Pepper, PepperColor } from './index';

const isPrimaryColor = (pepper:Pepper): Boolean => {
    return (pepper.color === PepperColor.Red || pepper.color === PepperColor.Blue || pepper.color === PepperColor.Yellow)
}

const isSecondaryColor = (pepper: Pepper): Boolean => {
    return (pepper.color === PepperColor.Orange || pepper.color === PepperColor.Purple || pepper.color == PepperColor.Green)
}

const isTertiaryColor = (pepper: Pepper): Boolean => {
    return (pepper.color === PepperColor.Black || pepper.color === PepperColor.White)
}

const isSameColors = (peppers: Pepper[]): Boolean => {
    return peppers[0].color === peppers[1].color
}
const hasBrown = (peppers: Pepper[]): Boolean => {
    return peppers.some((pepper) => pepper.color === PepperColor.Brown)
}

const hasPrimary = (peppers: Pepper[]): Boolean => {
    return peppers.some((pepper) => isPrimaryColor(pepper))
}
const hasSecondary = (peppers: Pepper[]): Boolean => {
    return peppers.some((pepper) => isSecondaryColor(pepper))
}
const hasTertiary = (peppers: Pepper[]): Boolean => {
    return peppers.some((pepper) => isTertiaryColor(pepper))
}
const hasPhantom = (peppers: Pepper[]): Boolean => {
    return peppers.some((pepper) => pepper.color === PepperColor.Ghost)
}
const hasRed = (peppers: Pepper[]): Boolean => {
    return peppers.some((pepper) => pepper.color === PepperColor.Red)
}
const hasBlue = (peppers: Pepper[]): Boolean => {
    return peppers.some((pepper) => pepper.color === PepperColor.Blue)
}
const hasYellow = (peppers: Pepper[]): Boolean => {
    return peppers.some((pepper) => pepper.color === PepperColor.Yellow)
}

const getNotThisColor = (peppers: Pepper[], avoidColors: PepperColor[]): Pepper => {
    let resultingPepper
    peppers.forEach((pepper) => {
        if (!avoidColors.some(color => color === pepper.color)) {
            resultingPepper = { ...pepper }
        }
    })
    return resultingPepper
}
// probably a better way to break this up, maybe using a hash to just count the number of 
// peppers should be an array of length 2 | 1 | 0
export const harvestPeppers = (peppers: Pepper[]): Pepper[] => {
    if (peppers.length !== 2) {
        return []
    }

    if (isSameColors(peppers)) {
        // all 2 of same color
        if (isSecondaryColor(peppers[0])) {
            // 2 same color secondaries
            return [{ kind: 'pepper', color: PepperColor.Black }]
        }
        // all other 2 of same cases
        return [{...peppers[0], ...peppers[0]}]
    } else {
        if (isSecondaryColor(peppers[0]) && isSecondaryColor(peppers[1])) {
            // two different colored 2ndary peppers
            return [{ kind: 'pepper', color: PepperColor.White }]
        }
        if (hasBrown(peppers)) {
            // double brown is covered line 60
            if (hasPrimary(peppers) || hasSecondary(peppers)) {
                return []
            }
            if (hasTertiary(peppers)) {
                // brown + black/white
                return [getNotThisColor(peppers, [PepperColor.Brown])]
            }
            // brown + phantom
            return [{ color: PepperColor.Brown, kind: 'pepper'}, { color: PepperColor.Brown, kind: 'pepper'}]

        }
        if (hasTertiary(peppers)) {
            // black/white + brown is covered on line 67
            // 2 black or 2 white is covered line 60
            //black/white + primary secondary
            if (hasPrimary(peppers) || hasSecondary(peppers)) {
                return [getNotThisColor(peppers, [PepperColor.White, PepperColor.Black])]
            }
            if (hasPhantom(peppers)) {
                // black/white + Ghost
                const resultPepper = getNotThisColor(peppers, [PepperColor.Ghost])
                return [{...resultPepper}, {...resultPepper}]
            }
            // black + white
            return [{ color: PepperColor.Ghost, kind: 'pepper' }]
        }
        if (hasPhantom(peppers)) {
            // double ghost is line 60
            // brown ghost is line 74
            // black/white ghost is line 87
            if (hasPrimary(peppers)) {
                return [{ kind: 'pepper', color: PepperColor.White }]
            }
            return [{ kind: 'pepper', color: PepperColor.Black }]
        }
        if (hasPrimary(peppers)) {
            if (hasSecondary(peppers)) {
                // primary + secondary
                return [{ color: PepperColor.Brown, kind: 'pepper' }]
            }
            // all doubles line 60
            
            // all primary mixes
            if (hasRed(peppers)) {
                if (hasBlue(peppers)) {
                    return [{ color: PepperColor.Purple, kind: 'pepper'}]
                }
                return [{ color: PepperColor.Orange, kind: 'pepper' }]
            }
            return [{ color: PepperColor.Green, kind: 'pepper'}]
        }
    }

    //     if ((isPrimaryColor(pepper1) && isSecondaryColor(pepper2)) ||(isPrimaryColor(pepper2) && isSecondaryColor(pepper1))) {
    //         return [{ kind: "pepper", color: PepperColor.Brown }]
    //     }


    //     if (pepper1.color === PepperColor.Brown && isPrimaryorSecondary(pepper2) || pepper2.color === PepperColor.Brown && isPrimaryorSecondary(pepper1)) {
    //         // brown and r/b/y/g/o/p pepper
    //         return []
    //     }
    // }

}