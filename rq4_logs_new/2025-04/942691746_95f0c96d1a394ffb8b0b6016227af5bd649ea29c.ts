import { Duck } from '../Types/types';
import { getRandomMovementType, getRandomDirection, getRandomPosition } from '../Ultils/Ultils';
import { GAME_CONSTANTS } from '../Constant/constant';

export const redDucks: Duck[] = [];


export function updateDucksBasedOnCount(): void {
    const duckCount = parseInt(localStorage.getItem('redDuckCount') || GAME_CONSTANTS.DUCK.DEFAULT_COUNT.toString());
    const currentDuckCount = redDucks.length;
    // Add more ducks if needed
    if (duckCount > currentDuckCount) {
        for (let i = currentDuckCount; i < duckCount; i++) {
            createNewDuck(i + 1);
        }
    }
    // Remove ducks if there are too many
    else if (duckCount < currentDuckCount && duckCount >= GAME_CONSTANTS.DUCK.DEFAULT_COUNT) {
        removeExcessDucks(currentDuckCount - duckCount);
    }
}

// Function to initialize ducks based on the count in localStorage
function createNewDuck(index: number): void {
    const position = getRandomPosition();
    const movementType = getRandomMovementType();
    
    // Create a new duck with randomized properties
    const newDuck: Duck = {
        id: `redduck${index}`,
        size: 100,
        position: position,
        direction: getRandomDirection(),
        speed: 0.2 + Math.random() * 0.2,
        frame: 1,
        moving: true,
        inPond: false,
        movementType: movementType,
        autoMoveInterval: undefined,
        selectedBasket: null
    };
    
    // Add movement-specific properties
    initializeMovementProperties(newDuck);
    
    // Add to ducks array
    redDucks.push(newDuck);
    
    // Create DOM element
    createDuckElement(newDuck);
}

function initializeMovementProperties(duck: Duck): void {
    if (duck.movementType === "circular") {
        duck.centerPoint = { left: duck.position.left, top: duck.position.top };
        duck.radius = 10 + Math.random() * 10;
        duck.pathProgress = Math.random() * Math.PI * 2;
    } else if (duck.movementType === "zigzag") {
        duck.zigzagAmplitude = 3 + Math.random() * 5;
    }
}

function createDuckElement(duck: Duck): void {
    const duckElement = document.createElement('img');
    duckElement.id = duck.id;
    duckElement.classList.add('redduck');
    duckElement.src = `../../assets/duck/right-left-red/a${duck.direction.x > 0 ? 1 : 3}.png`;
    duckElement.style.position = 'absolute';
    duckElement.style.width = '100px';
    duckElement.style.left = `${duck.position.left}%`;
    duckElement.style.top = `${duck.position.top}%`;
    duckElement.style.cursor = 'pointer';
    // Add to DOM
    document.body.appendChild(duckElement);
}

function removeExcessDucks(count: number): void {
    for (let i = 0; i < count; i++) {
        const duckToRemove = redDucks.pop();
        if (duckToRemove) {
            const element = document.getElementById(duckToRemove.id);
            if (element) element.remove();
            
            // Clear any timers for the removed duck
            clearDuckTimers(duckToRemove);
        }
    }
}

function clearDuckTimers(duck: Duck): void {
    if (duck.autoMoveInterval) clearTimeout(duck.autoMoveInterval);
    if (duck.relaxTimer1) clearTimeout(duck.relaxTimer1);
    if (duck.relaxTimer2) clearTimeout(duck.relaxTimer2);
}

// Configuration functions
export function changeDuckMovementType(duckId: string, newMovementType: Duck["movementType"]): void {
    const duck = redDucks.find(d => d.id === duckId);
    if (!duck) return;
    duck.movementType = newMovementType;
    // Reset any movement-specific properties
    if (newMovementType === "circular") {
        duck.centerPoint = { 
            left: duck.position.left, 
            top: duck.position.top 
        };
        duck.radius = 15;
        duck.pathProgress = 0;
    }
}
