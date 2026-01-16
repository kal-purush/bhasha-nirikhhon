// GLOBAL FUNCTION
function $(selector) {
  return document.querySelector(selector);
}

// FRAME CONSTANTS
// globalframeRate setting
let globalframeRate = 30; // ANIMATION GOOD AT 30 FPS
const ACCELERATE_FRAME_RATE = 500;
// Main animation duration setting
const LOGO_1_AND_MAIN_STAR_APPARITIONF_RAME = 30;
const END_OF_MAIN_STAR_ANIMATION_FRAME =
  LOGO_1_AND_MAIN_STAR_APPARITIONF_RAME + 15;
const FROM_LOGO_2_TO_LOGO_3_ANIMATION_FRAME =
  END_OF_MAIN_STAR_ANIMATION_FRAME + 4;
const TO_LOGO_4_ANIMATION_FRAME = FROM_LOGO_2_TO_LOGO_3_ANIMATION_FRAME + 4;
const LITTLE_STARS_STARTING_FRAME = TO_LOGO_4_ANIMATION_FRAME + 2;
const LITTLE_STARS_SECOND_WAVE_FRAME = LITTLE_STARS_STARTING_FRAME + 40;
const POKEMON_ARE_COMING_FRAME = LITTLE_STARS_SECOND_WAVE_FRAME + 70;
const POKEMON_ARE_STARTING_BATTLE_FRAME = POKEMON_ARE_COMING_FRAME + 30;
const POKEMON_ARE_ENDING_BATTLE_FRAME = POKEMON_ARE_STARTING_BATTLE_FRAME + 180;

// ************************************************************************
// ************************************************************************
// Logo animation
function showLogo(logoNumber) {
  let logoDOMElem = $(`#logo-anim-${logoNumber}`);
  logoDOMElem.style.visibility = "visible";
}
function hideLogo(logoNumber) {
  let logoDOMElem = $(`#logo-anim-${logoNumber}`);
  logoDOMElem.style.visibility = "hidden";
}

// ************************************************************************
// ************************************************************************
// MAIN STAR CONSTANTS
const MAIN_STAR_END_TOP = 100;
const MAIN_STAR_END_RIGHT = 100;

// Main star animation
let mainStartDOMElem = $(`#main-star`);
function showMainStar() {
  mainStartDOMElem.style.visibility = "visible";
}
function hideMainStar() {
  mainStartDOMElem.style.visibility = "hidden";
}
function animateMainStar(frame) {
  mainStartDOMElem.style.top = `${
    MAIN_STAR_END_TOP *
    ((frame - LOGO_1_AND_MAIN_STAR_APPARITIONF_RAME) /
      (END_OF_MAIN_STAR_ANIMATION_FRAME -
        LOGO_1_AND_MAIN_STAR_APPARITIONF_RAME))
  }%`;
  mainStartDOMElem.style.right = `${
    MAIN_STAR_END_RIGHT *
    ((frame - LOGO_1_AND_MAIN_STAR_APPARITIONF_RAME) /
      (END_OF_MAIN_STAR_ANIMATION_FRAME -
        LOGO_1_AND_MAIN_STAR_APPARITIONF_RAME))
  }%`;
}

// ************************************************************************
// ************************************************************************
// POPPING NAMES CONSTANTS
const POPPING_NAME_LIFETIME = 40;
const POPPING_NAME_FLASHING_INVERSE_RATE = 3;
// Popping names
let littleStarContainerDOMElem = $("#little-stars-container");

function summonPoppingName(coordX, coordY, promoName) {
  let poppingNameAnimationCounter = 0;
  let newPoppingName = document.createElement("div");
  newPoppingName.innerHTML = promoName;
  newPoppingName.setAttribute("class", "popping-name");
  newPoppingName.style.top = `${coordY}%`;
  newPoppingName.style.left = `${coordX}%`;
  littleStarContainerDOMElem.appendChild(newPoppingName);

  function animateAndClosePoppingName() {
    poppingNameAnimationCounter++;
    if (poppingNameAnimationCounter % POPPING_NAME_FLASHING_INVERSE_RATE == 0) {
      newPoppingName.style.color = "#E0CC74";
    } else {
      newPoppingName.style.color = "black";
    }
    if (poppingNameAnimationCounter > POPPING_NAME_LIFETIME) {
      newPoppingName.style.visibility = "hidden";
      return;
    }
    setTimeout(animateAndClosePoppingName, 1000 / globalframeRate);
  }
  animateAndClosePoppingName();
}

// ************************************************************************
// ************************************************************************
// LITTLE STARS CONSTANTS
const LITTLE_STARS_SPEED = 1.2;
const FLASHING_LITTLE_STARS_INVERSE_RATE = 3;
// Little stars animation
function LittleStars(
  number,
  color,
  startingX,
  startingY,
  isLittleStarFlashing,
  initFrame,
  promoName
) {
  this.initFrame = initFrame;
  this.isLittleStarFlashing = isLittleStarFlashing;
  this.img = new Image();
  this.img.src = `star-${color}.svg`;
  this.newLittleStar = littleStarContainerDOMElem.appendChild(this.img);
  this.newLittleStar.setAttribute("class", "little-stars");
  this.newLittleStar.setAttribute("id", `little-star-${number}`);
  this.newLittleStar.style.top = `${startingY}%`;
  this.newLittleStar.style.left = `${startingX}%`;
  this.coordX = startingX;
  this.promoName = promoName;

  if (this.coordX <= 50) {
    this.poppingToTheleft = true;
  } else {
    this.poppingToTheleft = false;
  }

  this.coordY = 0;
  this.littleStarIsClicked = false;
  this.poppingCounter = 0;
  this.timeToPop = 15 + 6 * (Math.random() - 0.5);

  this.moveLittleStar = (frame) => {
    if (
      this.isLittleStarFlashing &&
      frame % FLASHING_LITTLE_STARS_INVERSE_RATE == 0 &&
      !this.littleStarIsClicked
    ) {
      this.newLittleStar.style.visibility = "hidden";
    } else if (!this.littleStarIsClicked) {
      this.newLittleStar.style.visibility = "visible";
    }
    if (!this.littleStarIsClicked) {
      this.coordY = startingY + (frame - this.initFrame) * LITTLE_STARS_SPEED;
      this.newLittleStar.style.top = `${this.coordY}%`;
    }
  };

  this.newLittleStar.addEventListener("click", () => {
    this.littleStarIsClicked = true;
    this.newLittleStar.style.visibility = "visible";
    this.flyingToTheMoon();
  });

  this.flyingToTheMoon = () => {
    this.poppingCounter++;

    this.coordY += -this.poppingCounter;
    this.newLittleStar.style.top = `${this.coordY}%`;
    this.newLittleStar.style.height = `${16 + this.poppingCounter * 10}%`;
    this.newLittleStar.style.transform = `rotate(${
      this.poppingCounter * 1000
    }deg)`;

    if (this.poppingToTheleft) {
      this.coordX -= 8 * Math.cos(this.poppingCounter / 10) + Math.random() * 2;
      this.newLittleStar.style.left = `${this.coordX}%`;
    } else {
      this.coordX += 8 * Math.cos(this.poppingCounter / 10) + Math.random() * 2;
      this.newLittleStar.style.left = `${this.coordX}%`;
    }

    if (this.poppingCounter > this.timeToPop) {
      this.newLittleStar.style.visibility = "hidden";
      return summonPoppingName(this.coordX, this.coordY, this.promoName);
    }
    setTimeout(this.flyingToTheMoon, 1000 / globalframeRate);
  };

  this.hideLittleStars = () => {
    this.newLittleStar.style.visibility = "hidden";
  };
}

// ************************************************************************
// ************************************************************************
//POKEMON ASSETS CONSTANTS
const BATTLE_NIDO_JUMP_RATE_DURATION = 0.06;
const BATTLE_NIDO_FIRST_PAUSE_RATE_DURATION = 0.06;
const BATTLE_NIDO_PAUSE_JUMP_RATE_DURATION = 0.06;
const BATTLE_PAUSE_BEFORE_DUEL_RATE_DURATION = 0.1;
const BATTLE_ECTO_HEADBUTT_RUN_UP_RATE_DURATION = 0.15;
const BATTLE_ECTO_HEADBUTT_LAUNCH_RATE_DURATION = 0.03;
const BATTLE_NIDO_DODGE_HEADBUTT_AND_ECTO_RECOVER_FROM_IT_RATE_DURATION = 0.05;
const BATTLE_AFTER_HEADBUTT_PAUSE_RATE_DURATION = 0.03;
const BATTLE_NIDO_CHARGE_BUILD_UP_RATE_DURATION = 0.08;
const BATTLE_NIDO_CHARGE_RATE_DURATION = 0.06;

// Phase 1 to 5
const BATTLE_FIRST_PHASE_RATE_CHECKPOINT =
  BATTLE_NIDO_FIRST_PAUSE_RATE_DURATION; // nothing append
const BATTLE_SECOND_PHASE_RATE_CHECKPOINT =
  BATTLE_FIRST_PHASE_RATE_CHECKPOINT + BATTLE_NIDO_JUMP_RATE_DURATION; // nido move to the right
const BATTLE_THIRD_PHASE_RATE_CHECKPOINT =
  BATTLE_SECOND_PHASE_RATE_CHECKPOINT + BATTLE_NIDO_JUMP_RATE_DURATION; // nido move to the left
const BATTLE_FOURTH_PHASE_RATE_CHECKPOINT =
  BATTLE_THIRD_PHASE_RATE_CHECKPOINT + BATTLE_NIDO_PAUSE_JUMP_RATE_DURATION; // nido is lazy
const BATTLE_FIFTH_PHASE_RATE_CHECKPOINT =
  BATTLE_FOURTH_PHASE_RATE_CHECKPOINT + BATTLE_NIDO_JUMP_RATE_DURATION; // nido move to the right

// Phase 6 to 10
const BATTLE_SIXTH_PHASE_RATE_CHECKPOINT =
  BATTLE_FIFTH_PHASE_RATE_CHECKPOINT + BATTLE_NIDO_JUMP_RATE_DURATION; // nido move to the left
const BATTLE_SEVENTH_PHASE_RATE_CHECKPOINT =
  BATTLE_SIXTH_PHASE_RATE_CHECKPOINT + BATTLE_PAUSE_BEFORE_DUEL_RATE_DURATION; // nido and ecto pause before duel
const BATTLE_EIGHTH_PHASE_RATE_CHECKPOINT =
  BATTLE_SEVENTH_PHASE_RATE_CHECKPOINT +
  BATTLE_ECTO_HEADBUTT_RUN_UP_RATE_DURATION; // ecto runnin up for a fcking headbutt
const BATTLE_NINTH_PHASE_RATE_CHECKPOINT =
  BATTLE_EIGHTH_PHASE_RATE_CHECKPOINT +
  BATTLE_ECTO_HEADBUTT_LAUNCH_RATE_DURATION; // ecto launching his fcking headbutt
const BATTLE_TENTH_PHASE_RATE_CHECKPOINT =
  BATTLE_NINTH_PHASE_RATE_CHECKPOINT +
  BATTLE_NIDO_DODGE_HEADBUTT_AND_ECTO_RECOVER_FROM_IT_RATE_DURATION; // nido dodging ecto's headbutt and ecto recover from it

// Phase 11 to 15
const BATTLE_ELEVENTH_PHASE_RATE_CHECKPOINT =
  BATTLE_TENTH_PHASE_RATE_CHECKPOINT +
  BATTLE_AFTER_HEADBUTT_PAUSE_RATE_DURATION; // pause (nido don't recover before)
const BATTLE_TWELFTH_PHASE_RATE_CHECKPOINT =
  BATTLE_ELEVENTH_PHASE_RATE_CHECKPOINT + BATTLE_NIDO_JUMP_RATE_DURATION; // nido move to the left (instant recover before)
const BATTLE_THIRTEENTH_PHASE_RATE_CHECKPOINT =
  BATTLE_TWELFTH_PHASE_RATE_CHECKPOINT + BATTLE_NIDO_JUMP_RATE_DURATION; // nido move to the right
const BATTLE_FOURTEENTH_PHASE_RATE_CHECKPOINT =
  BATTLE_THIRTEENTH_PHASE_RATE_CHECKPOINT +
  BATTLE_NIDO_CHARGE_BUILD_UP_RATE_DURATION; // nido builds up his charge
const BATTLE_LAST_PHASE_RATE_CHECKPOINT =
  BATTLE_FOURTEENTH_PHASE_RATE_CHECKPOINT +
  BATTLE_NIDO_CHARGE_BUILD_UP_RATE_DURATION; // nido charges

// => printing last phase rate checkpoint (SHOULD BE <= 1)
console.log(
  `LAST PHASE RATE CHECKPOINT :: ${BATTLE_LAST_PHASE_RATE_CHECKPOINT.toFixed(
    3
  )} (should be <= 1)`
);

const BATTLE_NEXT_PHASE_RATE_THRESHOLD = 0.005;

const ECTO_END_RIGHT = 70;
const ECTO_STARTING_HEIGHT = 55;

const ECTO_HEADBUTT_STARTING_ANGLE = 0;
const ECTO_HEADBUTT_RUN_UP_ANGLE = -10;
const ECTO_HEADBUTT_STARTING_BOTTOM = 13;
const ECTO_HEADBUTT_RUN_UP_BOTTOM_DECREASE = -6;
const ECTO_HEADBUTT_RUN_UP_HEIGHT_INCREASE = 15;
const ECTO_HEADBUTT_RUN_UP_BACKSTEP_RANGE = 0.2;
const ECTO_HEADBUTT_LAUNCH_ANGLE_OVERSHOT = 4;

const NIDO_END_LEFT = 60;
const NIDO_STARTING_ANGLE = 0;
const NIDO_STARTING_Y_COORD = 36;
const NIDO_STARTING_HEIGHT = 35;

const NIDO_BATTLE_BASIC_JUMP_X_STEP_RANGE = -5;
const NIDO_BASIC_JUMP_HEIGHT = 10;

const NIDO_DODGE_JUMP_HEIGHT = 15;
const NIDO_BATTLE_DODGE_JUMP_X_STEP_RANGE = -10;
const NIDO_DODGE_JUMP_ANGLE = -10;

const NIDO_CHARGE_BUILD_UP_ANGLE = -5;
const NIDO_CHARGE_BUILD_UP_HEIGHT_DECREASE = -2;
const NIDO_CHARGE_BUILD_UP_X_STEP_RANGE = 5;

const NIDO_CHARGE_HEIGHT_DECREASE = 25;
const NIDO_CHARGE_X_STEP_RANGE = -60;

// Pokemon assets animation
let ectoDOMElem = $(`#ectoplasma-img`);
let ectoEndPhaseXcoord = ECTO_END_RIGHT;
let ectoEndPhaseAngle = 0;
let ectoXcoord = 0;

let nidoDOMElem = $(`#nidorino-img`);
let nidoEndPhaseXcoord = NIDO_END_LEFT;
let nidoEndPhaseAngle = 0;
let nidoEndPhaseHeight = NIDO_STARTING_HEIGHT;
let nidoXcoord = 0;
let nidoYcoord = 0;

function showPokemons() {
  ectoDOMElem.style.visibility = "visible";
  nidoDOMElem.style.visibility = "visible";
}

function ectoIsComingAnim(frame) {
  ectoXcoord =
    (ECTO_END_RIGHT * (frame - POKEMON_ARE_COMING_FRAME)) /
    (POKEMON_ARE_STARTING_BATTLE_FRAME - POKEMON_ARE_COMING_FRAME);
  ectoEndPhaseXcoord = ectoXcoord;
  ectoDOMElem.style.right = `${ectoXcoord}%`;
}

function nidoIsComingAnim(frame) {
  nidoXcoord =
    (NIDO_END_LEFT * (frame - POKEMON_ARE_COMING_FRAME)) /
    (POKEMON_ARE_STARTING_BATTLE_FRAME - POKEMON_ARE_COMING_FRAME);
  nidoEndPhaseXcoord = nidoXcoord;
  nidoDOMElem.style.left = `${nidoXcoord}%`;
}

function makeNidoJump(
  phaseRate,
  direction,
  jumpXDistance,
  jumpYDistance,
  isLastPhaseFrame
) {
  nidoYcoord =
    NIDO_STARTING_Y_COORD + jumpYDistance * Math.sin(phaseRate * Math.PI);

  switch (direction) {
    case "left":
      nidoXcoord = nidoEndPhaseXcoord + phaseRate * jumpXDistance;
      break;
    case "right":
      nidoXcoord = nidoEndPhaseXcoord - phaseRate * jumpXDistance;
      break;
  }

  if (isLastPhaseFrame) {
    console.log("it's dino jump last phase frame");
    nidoYcoord = NIDO_STARTING_Y_COORD;
    nidoEndPhaseXcoord = nidoXcoord;
  }

  nidoDOMElem.style.left = `${nidoXcoord}%`;
  nidoDOMElem.style.bottom = `${nidoYcoord}%`;
}

let phaseRate = 0; // 0 to 1

let battleProgressRateCounter = 0;
// |||||||||||||||||||||||||||||||||||||
// BATTLE ANIMATION
// |||||||||||||||||||||||||||||||||||||
function pokemonBattleAnim(frame) {
  battleProgressRateCounter =
    (frame - POKEMON_ARE_STARTING_BATTLE_FRAME) /
    (POKEMON_ARE_ENDING_BATTLE_FRAME - POKEMON_ARE_STARTING_BATTLE_FRAME); // 0 to 1

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // BATTLES PHASES
  // FIRST PHASE -> nothing append
  if (battleProgressRateCounter <= BATTLE_FIRST_PHASE_RATE_CHECKPOINT) {
    return;

    // SECOND PHASE -> nido move to the right
  } else if (battleProgressRateCounter <= BATTLE_SECOND_PHASE_RATE_CHECKPOINT) {
    phaseRate =
      (battleProgressRateCounter - BATTLE_FIRST_PHASE_RATE_CHECKPOINT) /
      (BATTLE_SECOND_PHASE_RATE_CHECKPOINT -
        BATTLE_FIRST_PHASE_RATE_CHECKPOINT); // 0 to 1

    makeNidoJump(
      phaseRate,
      "right",
      NIDO_BATTLE_BASIC_JUMP_X_STEP_RANGE,
      NIDO_BASIC_JUMP_HEIGHT,
      battleProgressRateCounter >
        BATTLE_SECOND_PHASE_RATE_CHECKPOINT - BATTLE_NEXT_PHASE_RATE_THRESHOLD
    );

    // THIRD PHASE -> nido move to the left
  } else if (battleProgressRateCounter <= BATTLE_THIRD_PHASE_RATE_CHECKPOINT) {
    phaseRate =
      (battleProgressRateCounter - BATTLE_SECOND_PHASE_RATE_CHECKPOINT) /
      (BATTLE_THIRD_PHASE_RATE_CHECKPOINT -
        BATTLE_SECOND_PHASE_RATE_CHECKPOINT); // 0 to 1

    makeNidoJump(
      phaseRate,
      "left",
      NIDO_BATTLE_BASIC_JUMP_X_STEP_RANGE,
      NIDO_BASIC_JUMP_HEIGHT,
      battleProgressRateCounter >
        BATTLE_THIRD_PHASE_RATE_CHECKPOINT - BATTLE_NEXT_PHASE_RATE_THRESHOLD
    );
    // FOURTH PHASE -> nido is lazy
  } else if (battleProgressRateCounter <= BATTLE_FOURTH_PHASE_RATE_CHECKPOINT) {
    return;

    // FIFTH PHASE -> nido move again to the right
  } else if (battleProgressRateCounter <= BATTLE_FIFTH_PHASE_RATE_CHECKPOINT) {
    phaseRate =
      (battleProgressRateCounter - BATTLE_FOURTH_PHASE_RATE_CHECKPOINT) /
      (BATTLE_FIFTH_PHASE_RATE_CHECKPOINT -
        BATTLE_FOURTH_PHASE_RATE_CHECKPOINT); // 0 to 1

    makeNidoJump(
      phaseRate,
      "right",
      NIDO_BATTLE_BASIC_JUMP_X_STEP_RANGE,
      NIDO_BASIC_JUMP_HEIGHT,
      battleProgressRateCounter >
        BATTLE_FIFTH_PHASE_RATE_CHECKPOINT - BATTLE_NEXT_PHASE_RATE_THRESHOLD
    );

    // SIXTH PHASE -> nido move again to the right
  } else if (battleProgressRateCounter <= BATTLE_SIXTH_PHASE_RATE_CHECKPOINT) {
    phaseRate =
      (battleProgressRateCounter - BATTLE_FIFTH_PHASE_RATE_CHECKPOINT) /
      (BATTLE_SIXTH_PHASE_RATE_CHECKPOINT - BATTLE_FIFTH_PHASE_RATE_CHECKPOINT); // 0 to 1

    makeNidoJump(
      phaseRate,
      "left",
      NIDO_BATTLE_BASIC_JUMP_X_STEP_RANGE,
      NIDO_BASIC_JUMP_HEIGHT,
      battleProgressRateCounter >
        BATTLE_SIXTH_PHASE_RATE_CHECKPOINT - BATTLE_NEXT_PHASE_RATE_THRESHOLD
    );

    // SEVENTH PHASE -> nido and ecto are waiting before the battle
  } else if (
    battleProgressRateCounter <= BATTLE_SEVENTH_PHASE_RATE_CHECKPOINT
  ) {
    return;

    // EIGHTH PHASE -> ecto charging headbutt
  } else if (battleProgressRateCounter <= BATTLE_EIGHTH_PHASE_RATE_CHECKPOINT) {
    phaseRate =
      (battleProgressRateCounter - BATTLE_SEVENTH_PHASE_RATE_CHECKPOINT) /
      (BATTLE_EIGHTH_PHASE_RATE_CHECKPOINT -
        BATTLE_SEVENTH_PHASE_RATE_CHECKPOINT); // 0 to 1

    ectoXcoord =
      ectoEndPhaseXcoord + phaseRate * ECTO_HEADBUTT_RUN_UP_BACKSTEP_RANGE;
    ectoEndPhaseXcoord = ectoXcoord;

    ectoDOMElem.style.transform = `rotate(${
      ECTO_HEADBUTT_STARTING_ANGLE + phaseRate * ECTO_HEADBUTT_RUN_UP_ANGLE
    }deg)`;

    ectoDOMElem.style.height = `${
      ECTO_STARTING_HEIGHT + phaseRate * ECTO_HEADBUTT_RUN_UP_HEIGHT_INCREASE
    }%`;

    ectoDOMElem.style.bottom = `${
      ECTO_HEADBUTT_STARTING_BOTTOM +
      phaseRate * ECTO_HEADBUTT_RUN_UP_BOTTOM_DECREASE
    }%`;

    ectoDOMElem.style.right = `${ectoXcoord}%`;

    // NINTH PHASE -> ecto launching headbutt
  } else if (battleProgressRateCounter <= BATTLE_NINTH_PHASE_RATE_CHECKPOINT) {
    phaseRate =
      (battleProgressRateCounter - BATTLE_EIGHTH_PHASE_RATE_CHECKPOINT) /
      (BATTLE_NINTH_PHASE_RATE_CHECKPOINT -
        BATTLE_EIGHTH_PHASE_RATE_CHECKPOINT); // 0 to 1

    ectoXcoord =
      ectoEndPhaseXcoord - phaseRate * ECTO_HEADBUTT_RUN_UP_BACKSTEP_RANGE;
    ectoEndPhaseXcoord = ectoXcoord;

    ectoEndPhaseAngle =
      ECTO_HEADBUTT_STARTING_ANGLE +
      ECTO_HEADBUTT_RUN_UP_ANGLE -
      (ECTO_HEADBUTT_RUN_UP_ANGLE - ECTO_HEADBUTT_LAUNCH_ANGLE_OVERSHOT) *
        phaseRate;
    ectoDOMElem.style.transform = `rotate(${ectoEndPhaseAngle}deg)`;

    ectoDOMElem.style.height = `${
      ECTO_STARTING_HEIGHT +
      ECTO_HEADBUTT_RUN_UP_HEIGHT_INCREASE -
      phaseRate * ECTO_HEADBUTT_RUN_UP_HEIGHT_INCREASE
    }%`;

    ectoDOMElem.style.bottom = `${
      ECTO_HEADBUTT_STARTING_BOTTOM +
      ECTO_HEADBUTT_RUN_UP_BOTTOM_DECREASE -
      phaseRate * ECTO_HEADBUTT_RUN_UP_BOTTOM_DECREASE
    }%`;

    ectoDOMElem.style.right = `${ectoXcoord}%`;

    // TENTH PHASE -> nido dodging ecto's headbutt and ecto recover from it
  } else if (battleProgressRateCounter <= BATTLE_TENTH_PHASE_RATE_CHECKPOINT) {
    phaseRate =
      (battleProgressRateCounter - BATTLE_NINTH_PHASE_RATE_CHECKPOINT) /
      (BATTLE_TENTH_PHASE_RATE_CHECKPOINT - BATTLE_NINTH_PHASE_RATE_CHECKPOINT); // 0 to 1

    ectoEndPhaseAngle =
      ECTO_HEADBUTT_STARTING_ANGLE +
      ECTO_HEADBUTT_LAUNCH_ANGLE_OVERSHOT -
      ECTO_HEADBUTT_LAUNCH_ANGLE_OVERSHOT * phaseRate;

    ectoDOMElem.style.transform = `rotate(${ectoEndPhaseAngle}deg)`;

    makeNidoJump(
      phaseRate,
      "right",
      NIDO_BATTLE_DODGE_JUMP_X_STEP_RANGE,
      NIDO_DODGE_JUMP_HEIGHT,
      battleProgressRateCounter >
        BATTLE_TENTH_PHASE_RATE_CHECKPOINT - BATTLE_NEXT_PHASE_RATE_THRESHOLD
    );

    nidoEndPhaseAngle = NIDO_STARTING_ANGLE + NIDO_DODGE_JUMP_ANGLE * phaseRate;
    nidoDOMElem.style.transform = `rotate(${nidoEndPhaseAngle}deg)`;

    // ELEVENTH PHASE -> pause
  } else if (
    battleProgressRateCounter <= BATTLE_ELEVENTH_PHASE_RATE_CHECKPOINT
  ) {
    return;

    // TWELFTH PHASE -> nido move to the left (instant recorver before)
  } else if (
    battleProgressRateCounter <= BATTLE_TWELFTH_PHASE_RATE_CHECKPOINT
  ) {
    phaseRate =
      (battleProgressRateCounter - BATTLE_ELEVENTH_PHASE_RATE_CHECKPOINT) /
      (BATTLE_TWELFTH_PHASE_RATE_CHECKPOINT -
        BATTLE_ELEVENTH_PHASE_RATE_CHECKPOINT); // 0 to 1

    nidoDOMElem.style.transform = `rotate(${NIDO_STARTING_ANGLE}deg)`;

    makeNidoJump(
      phaseRate,
      "left",
      NIDO_BATTLE_BASIC_JUMP_X_STEP_RANGE,
      NIDO_BASIC_JUMP_HEIGHT,
      battleProgressRateCounter >
        BATTLE_TWELFTH_PHASE_RATE_CHECKPOINT - BATTLE_NEXT_PHASE_RATE_THRESHOLD
    );

    // THIRDTEENTH PHASE -> nido move to the left (instant recover before)
  } else if (
    battleProgressRateCounter <= BATTLE_THIRTEENTH_PHASE_RATE_CHECKPOINT
  ) {
    phaseRate =
      (battleProgressRateCounter - BATTLE_TWELFTH_PHASE_RATE_CHECKPOINT) /
      (BATTLE_THIRTEENTH_PHASE_RATE_CHECKPOINT -
        BATTLE_TWELFTH_PHASE_RATE_CHECKPOINT); // 0 to 1

    makeNidoJump(
      phaseRate,
      "right",
      NIDO_BATTLE_BASIC_JUMP_X_STEP_RANGE,
      NIDO_BASIC_JUMP_HEIGHT,
      battleProgressRateCounter >
        BATTLE_THIRTEENTH_PHASE_RATE_CHECKPOINT -
          BATTLE_NEXT_PHASE_RATE_THRESHOLD
    );

    // FOURTEENTH PHASE -> nido builds up his charge
  } else if (
    battleProgressRateCounter <= BATTLE_FOURTEENTH_PHASE_RATE_CHECKPOINT
  ) {
    phaseRate =
      (battleProgressRateCounter - BATTLE_THIRTEENTH_PHASE_RATE_CHECKPOINT) /
      (BATTLE_FOURTEENTH_PHASE_RATE_CHECKPOINT -
        BATTLE_THIRTEENTH_PHASE_RATE_CHECKPOINT); // 0 to 1

    nidoEndPhaseAngle =
      NIDO_STARTING_ANGLE + NIDO_CHARGE_BUILD_UP_ANGLE * phaseRate;

    nidoEndPhaseHeight =
      NIDO_STARTING_HEIGHT + NIDO_CHARGE_BUILD_UP_HEIGHT_DECREASE * phaseRate;

    nidoXcoord =
      nidoEndPhaseXcoord + phaseRate * NIDO_CHARGE_BUILD_UP_X_STEP_RANGE;

    nidoDOMElem.style.transform = `rotate(${nidoEndPhaseAngle}deg)`;
    nidoDOMElem.style.height = `${nidoEndPhaseHeight}%`;
    nidoDOMElem.style.left = `${nidoXcoord}%`;

    // LAST PHASE -> nido charges
  } else if (battleProgressRateCounter <= BATTLE_LAST_PHASE_RATE_CHECKPOINT) {
    phaseRate =
      (battleProgressRateCounter - BATTLE_FOURTEENTH_PHASE_RATE_CHECKPOINT) /
      (BATTLE_LAST_PHASE_RATE_CHECKPOINT -
        BATTLE_FOURTEENTH_PHASE_RATE_CHECKPOINT); // 0 to 1

    nidoEndPhaseHeight =
      NIDO_STARTING_HEIGHT + NIDO_CHARGE_HEIGHT_DECREASE * phaseRate;

    nidoXcoord = nidoEndPhaseXcoord + phaseRate * NIDO_CHARGE_X_STEP_RANGE;

    nidoDOMElem.style.height = `${nidoEndPhaseHeight}%`;
    nidoDOMElem.style.left = `${nidoXcoord}%`;
  }
}
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

// ************************************************************************
// ************************************************************************
//Pause btn
let pauseEnable = false;
let lastLogoDOMElem = $(`#logo-anim-4`);
lastLogoDOMElem.addEventListener("click", () => {
  pauseEnable = !pauseEnable;
});

// ************************************************************************
// ************************************************************************
// SKIP BTN AND VIDEO ACCELERATOR INDICATOR CONTANTS
const SKIP_BTN_INTERVAL_TIME = 400;
const VIDEO_ACCELERATOR_INDICATOR_INTERVAL_TIME = 200;

// Skip btn variables
let skipBtnIsClicked = false;
let skipBtnDOMElem = $(`#skip-btn`);
let skipBtnOpacity = 0;
let isMouseOnSkipBtn = false;
let skipBtnInterval;

// Video accelerator indicator variables
let videoAcceleratorIndicatorDOMElem = $(`#video-accelerator-indicator`);
let videoAcceleratorIndicatorInterval;

// Corresponding functions
function showSkipBtn() {
  if (skipBtnOpacity > 0.4 || isMouseOnSkipBtn) {
    return;
  }
  skipBtnOpacity += 0.01;
  skipBtnDOMElem.style.opacity = skipBtnOpacity;
  setTimeout(showSkipBtn, 1000 / globalframeRate);
}

skipBtnDOMElem.addEventListener("click", () => {
  skipBtnIsClicked = !skipBtnIsClicked;

  // TO UNCOMMENT TO CHANGE SKIP BTN BEHAVIOR FOR ACCELERATE ANIMATION
  // ++++++++++++++++++++++++++++++++++++++++++++++++++++
  globalframeRate = ACCELERATE_FRAME_RATE;
  videoAcceleratorIndicatorDOMElem.style.visibility = "visible";
  videoAcceleratorIndicatorInterval = setInterval(
    launchAccelerationVisualIndicator,
    VIDEO_ACCELERATOR_INDICATOR_INTERVAL_TIME
  );
  // ++++++++++++++++++++++++++++++++++++++++++++++++++++
});
function launchAccelerationVisualIndicator() {
  if (videoAcceleratorIndicatorDOMElem.textContent == ">") {
    videoAcceleratorIndicatorDOMElem.innerHTML = ">>";
  } else if (videoAcceleratorIndicatorDOMElem.textContent == ">>") {
    videoAcceleratorIndicatorDOMElem.innerHTML = ">>>";
  } else {
    videoAcceleratorIndicatorDOMElem.innerHTML = ">";
  }
}

skipBtnDOMElem.addEventListener("mouseover", () => {
  isMouseOnSkipBtn = true;
  skipBtnDOMElem.style.opacity = 1;
  switchSkipBtnTextContent();
  skipBtnInterval = setInterval(
    switchSkipBtnTextContent,
    SKIP_BTN_INTERVAL_TIME
  );
});
function switchSkipBtnTextContent() {
  if (skipBtnDOMElem.textContent == "SKIP >") {
    skipBtnDOMElem.innerHTML = "SKIP >>";
  } else if (skipBtnDOMElem.textContent == "SKIP >>") {
    skipBtnDOMElem.innerHTML = "SKIP >>>";
  } else {
    skipBtnDOMElem.innerHTML = "SKIP >";
  }
}

skipBtnDOMElem.addEventListener("mouseout", () => {
  skipBtnDOMElem.style.opacity = 0.4;
  clearInterval(skipBtnInterval);
  skipBtnDOMElem.innerHTML = "SKIP >";
});

// ************************************************************************
// ************************************************************************
// GOT TO NEXT PAGE CONSTANTS
const GO_TO_NEXT_PAGE_SPEED = 1;

// Go to next page animation
let blackTopCrossBarDOMElem = $("#top-section");
let blackTopCrossBarTop = 0;
let blackBottomCrossBarDOMElem = $("#bottom-section");
let blackBottomCrossBarBottom = 0;
let goToNextPageCounter = 0;
let fadingPokemonOpacity = 0;

function goToNextPage() {
  goToNextPageCounter++;
  if (goToNextPageCounter > 25) {
    clearInterval(videoAcceleratorIndicatorInterval);
    videoAcceleratorIndicatorDOMElem.style.visibility = "hidden";
    // window.location.href = "./index2.html"; // PAGE REDIRECTION HERE †††††††††††††††
    return;
  }

  fadingPokemonOpacity = 1 - goToNextPageCounter / 25;
  ectoDOMElem.style.opacity = fadingPokemonOpacity;
  nidoDOMElem.style.opacity = fadingPokemonOpacity;

  blackTopCrossBarTop -= GO_TO_NEXT_PAGE_SPEED;
  blackTopCrossBarDOMElem.style.top = `${blackTopCrossBarTop}%`;

  blackBottomCrossBarBottom -= GO_TO_NEXT_PAGE_SPEED;
  blackBottomCrossBarDOMElem.style.bottom = `${blackBottomCrossBarBottom}%`;
  setTimeout(goToNextPage, 1000 / globalframeRate);
}

// ************************************************************************
// ************************************************************************
// ************************************************************************
// ************************************************************************
// ************************************************************************
// ************************************************************************
// ************************************************************************
// ************************************************************************
// Main animation loop
let currentFrame = 0; // SHOULD BE ZERO (MORE IS FOR TESTING PURPOSES => 150 GO DIRECT TO BATTLE)

let allLittleStars = [];
let littleStarsNb = 0;
function animate() {
  // // TO UNCOMMENT TO CHANGE SKIP BTN BEHAVIOR FOR DIRECT ANIMATION SKIPING
  // // ++++++++++++++++++++++++++++++++++++++++++++++++++++
  // if (skipBtnIsClicked) {
  //   allLittleStars.forEach((littleStar) => littleStar.hideLittleStars());
  //   pauseEnable = !pauseEnable;
  //   goToNextPage();
  //   return;
  // }
  // // ++++++++++++++++++++++++++++++++++++++++++++++++++++

  if (!pauseEnable) {
    currentFrame++;
  }

  switch (true) {
    case currentFrame == LOGO_1_AND_MAIN_STAR_APPARITIONF_RAME:
      showLogo(1);
      showMainStar();
      break;

    case currentFrame > LOGO_1_AND_MAIN_STAR_APPARITIONF_RAME &&
      currentFrame < END_OF_MAIN_STAR_ANIMATION_FRAME:
      animateMainStar(currentFrame);
      break;

    case currentFrame == END_OF_MAIN_STAR_ANIMATION_FRAME:
      hideMainStar();
      hideLogo(1);
      showLogo(2);
      break;

    case currentFrame == FROM_LOGO_2_TO_LOGO_3_ANIMATION_FRAME:
      hideLogo(2);
      showLogo(3);
      break;

    case currentFrame == TO_LOGO_4_ANIMATION_FRAME:
      hideLogo(3);
      showLogo(4);
      break;

    case currentFrame == LITTLE_STARS_STARTING_FRAME:
      showSkipBtn();
      let firstLittleStarsWaveColumns = [10, 30, 53, 86];
      let firstLittleStarRow = 2;
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "orange",
          firstLittleStarsWaveColumns[0],
          firstLittleStarRow,
          false,
          currentFrame,
          "BRIAC"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "red",
          firstLittleStarsWaveColumns[0] - 4,
          firstLittleStarRow + 10,
          true,
          currentFrame,
          "AMANDA"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "orange",
          firstLittleStarsWaveColumns[1],
          firstLittleStarRow,
          false,
          currentFrame,
          "NAOUAL"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "red",
          firstLittleStarsWaveColumns[1] - 4,
          firstLittleStarRow + 10,
          true,
          currentFrame,
          "IMEN"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "default",
          firstLittleStarsWaveColumns[2],
          firstLittleStarRow,
          false,
          currentFrame,
          "CHARLY"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "orange",
          firstLittleStarsWaveColumns[2] - 4,
          firstLittleStarRow + 10,
          true,
          currentFrame,
          "IBROHIM"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "lightblue",
          firstLittleStarsWaveColumns[3],
          firstLittleStarRow,
          false,
          currentFrame,
          "YASMINE"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "blue",
          firstLittleStarsWaveColumns[3] - 4,
          firstLittleStarRow + 10,
          true,
          currentFrame,
          "HELENE"
        )
      );

      break;

    case currentFrame == LITTLE_STARS_SECOND_WAVE_FRAME:
      let secondLittleStarsWaveColumns = [20, 40, 53, 79];
      let secondLittleStarRow = 2;

      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "orange",
          secondLittleStarsWaveColumns[0],
          secondLittleStarRow,
          false,
          currentFrame,
          "SOUMAYA"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "red",
          secondLittleStarsWaveColumns[0] - 4,
          secondLittleStarRow + 10,
          true,
          currentFrame,
          "MARVIN"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "green",
          secondLittleStarsWaveColumns[1],
          secondLittleStarRow,
          false,
          currentFrame,
          "GUILLAUME"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "lightblue",
          secondLittleStarsWaveColumns[1] - 4,
          secondLittleStarRow + 10,
          true,
          currentFrame,
          "BENOIT"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "default",
          secondLittleStarsWaveColumns[2],
          secondLittleStarRow,
          false,
          currentFrame,
          "NAHIM"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "brown",
          secondLittleStarsWaveColumns[2] - 4,
          secondLittleStarRow + 10,
          true,
          currentFrame,
          "JOHAN"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "lightblue",
          secondLittleStarsWaveColumns[3],
          secondLittleStarRow,
          false,
          currentFrame,
          "ABIBA"
        )
      );
      allLittleStars.push(
        new LittleStars(
          ++littleStarsNb,
          "blue",
          secondLittleStarsWaveColumns[3] - 4,
          secondLittleStarRow + 10,
          true,
          currentFrame,
          "KEVIN"
        )
      );

      break;

    case currentFrame < POKEMON_ARE_COMING_FRAME:
      allLittleStars.forEach((littleStar) => {
        littleStar.moveLittleStar(currentFrame);
      });
      break;

    case currentFrame == POKEMON_ARE_COMING_FRAME:
      hideLogo(4);
      showPokemons();
      break;

    case currentFrame < POKEMON_ARE_STARTING_BATTLE_FRAME:
      ectoIsComingAnim(currentFrame);
      nidoIsComingAnim(currentFrame);
      break;

    case currentFrame < POKEMON_ARE_ENDING_BATTLE_FRAME:
      pokemonBattleAnim(currentFrame);
      break;

    case true:
      allLittleStars.forEach((littleStar) => littleStar.hideLittleStars());
      pauseEnable = !pauseEnable;
      goToNextPage();
      return;

    default:
      break;
  }

  setTimeout(animate, 1000 / globalframeRate);
}

// LET'S GO
animate();