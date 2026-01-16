import { createCritAnimation } from "../pf2e-rpg-numbers/scripts/helpers/animation/crit/critAnimation.js";

Hooks.on("renderTokenConfig", (app, html, data) => {

  setTimeout(() => {

    const container = document.querySelector('div[data-tab="pf2e-rpg-numbers"]');

    const button = document.createElement('button');
    button.textContent = "Test persona animation";
    button.classList.add('my-custom-button');
    button.id = "test-animation-p";
    button.style.display = "block";
    button.style.margin = "1rem auto";
    button.style.width = "24rem";
    container.appendChild(button);

    const button2 = document.createElement('button');
    button2.textContent = "Test fire emblem animation";
    button2.classList.add('my-custom-button');
    button2.id = "test-animation-f";
    button2.style.display = "block";
    button2.style.margin = "1rem auto";
    button2.style.width = "24rem";
    container.appendChild(button2);

    html.find('#test-animation-p').click((e) => {
      e.preventDefault();

      let tokenPf2eAnimation = getTestData(app);

      createCritAnimation({ type: "custom", whisper: [game.userId], token: tokenPf2eAnimation.document }, "persona");

    });

    html.find('#test-animation-f').click((e) => {
      e.preventDefault();

      let tokenPf2eAnimation = getTestData(app);

      createCritAnimation({ type: "custom", whisper: [game.userId], token: tokenPf2eAnimation.document }, "fire-emblem");

    });

  }, 1000);
});

function getTestData(app) {


  let crix = parseFloat(document.getElementsByName('flags.pf2e-rpg-numbers.critOffsetX')[0].value);
  let criy = parseFloat(document.getElementsByName('flags.pf2e-rpg-numbers.critOffsetY')[0].value);
  let cris = parseFloat(document.getElementsByName('flags.pf2e-rpg-numbers.critScale')[0].value);
  let crir = parseFloat(document.getElementsByName('flags.pf2e-rpg-numbers.critRotation')[0].value);
  let rotationOffset = parseFloat(document.getElementsByName('flags.pf2e-rpg-numbers.rotationOffset')[0].value);

  let critSFX = document.getElementsByName('flags.pf2e-rpg-numbers.critSFX')[0].value;
  let fireEmblemImg = document.getElementsByName('flags.pf2e-rpg-numbers.fireEmblemImg')[0].value;
  let personaImg = document.getElementsByName('flags.pf2e-rpg-numbers.personaImg')[0].value;

  let tokenPf2eAnimation = canvas.tokens.get(app.token.id)

  tokenPf2eAnimation.document.flags["pf2e-rpg-numbers"].critOffsetX = crix;
  tokenPf2eAnimation.document.flags["pf2e-rpg-numbers"].critOffsetY = criy;
  tokenPf2eAnimation.document.flags["pf2e-rpg-numbers"].critScale = cris;
  tokenPf2eAnimation.document.flags["pf2e-rpg-numbers"].critRotation = crir;
  tokenPf2eAnimation.document.flags["pf2e-rpg-numbers"].critSFX = critSFX;
  tokenPf2eAnimation.document.flags["pf2e-rpg-numbers"].fireEmblemImg = fireEmblemImg;
  tokenPf2eAnimation.document.flags["pf2e-rpg-numbers"].personaImg = personaImg;
  tokenPf2eAnimation.document.flags["pf2e-rpg-numbers"].rotationOffset = rotationOffset;


  return tokenPf2eAnimation;

}