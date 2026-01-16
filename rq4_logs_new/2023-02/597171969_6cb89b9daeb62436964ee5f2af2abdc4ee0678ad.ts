import { circleCollidesWithRectangle } from "./circle-collides-with-rectangle";
import { Sound } from "./sound-class";
import { CollisionType } from "./types";
import {
  context,
  canvasErrorString,
  canvas,
  keys,
  lastKey,
  boundaries,
  powerUps,
  scoreElement,
  pellets,
  eatPelletSound,
  paused,
} from "./main";
import { ghosts } from "./ghosts";
import { Player } from "./player-class";
import { Ghost } from "./ghost-class";

type AnimateType = {
  animationId: number;
  ghosts: Ghost[];
  player: Player;
  score: number;
};

// BEG ANIMATE
export function animate({ animationId, player, score }: AnimateType) {
  animationId = requestAnimationFrame(
    animate.bind(null, { animationId, ghosts, player, score })
  );

  // Use our guard clause again
  if (!context) {
    console.error(canvasErrorString);
    return;
  }

  // Clear the canvas so our last player position is not shown.
  // Only the updated player position
  context.clearRect(0, 0, canvas.width, canvas.height);

  // Character movement
  if (keys.ArrowUp.pressed && lastKey === "ArrowUp") {
    for (let i = 0; i < boundaries.length; i++) {
      const boundary = boundaries[i];
      if (
        circleCollidesWithRectangle({
          circle: { ...player, velocity: { x: 0, y: -5 } },
          rectangle: boundary,
        })
      ) {
        player.velocity.y = 0;
        break;
      } else {
        player.velocity.y = -5;
      }
    }
  } else if (keys.ArrowDown.pressed && lastKey === "ArrowDown") {
    for (let i = 0; i < boundaries.length; i++) {
      const boundary = boundaries[i];
      if (
        circleCollidesWithRectangle({
          circle: { ...player, velocity: { x: 0, y: 5 } },
          rectangle: boundary,
        })
      ) {
        player.velocity.y = 0;
        break;
      } else {
        player.velocity.y = 5;
      }
    }
  } else if (keys.ArrowLeft.pressed && lastKey === "ArrowLeft") {
    for (let i = 0; i < boundaries.length; i++) {
      const boundary = boundaries[i];
      if (
        circleCollidesWithRectangle({
          circle: { ...player, velocity: { x: -5, y: 0 } },
          rectangle: boundary,
        })
      ) {
        player.velocity.x = 0;
        break;
      } else {
        player.velocity.x = -5;
      }
    }
  } else if (keys.ArrowRight.pressed && lastKey === "ArrowRight") {
    for (let i = 0; i < boundaries.length; i++) {
      const boundary = boundaries[i];
      if (
        circleCollidesWithRectangle({
          circle: { ...player, velocity: { x: 5, y: 0 } },
          rectangle: boundary,
        })
      ) {
        player.velocity.x = 0;
        break;
      } else {
        player.velocity.x = 5;
      }
    }
  }

  // Detect power ups collison
  for (
    let powerUpIndex = powerUps.length - 1;
    0 <= powerUpIndex;
    powerUpIndex--
  ) {
    const powerUp = powerUps[powerUpIndex];
    powerUp.draw();

    // Player collides with power up
    if (
      Math.hypot(
        powerUp.position.x - player.position.x,
        powerUp.position.y - player.position.y
      ) <
      powerUp.radius + player.radius
    ) {
      powerUps.splice(powerUpIndex, 1);
      ghosts.forEach((ghost) => {
        // const originalColor = ghost.color;
        ghost.scared = true;

        setInterval(() => {
          ghost.blinking = true;
        }, 500);

        // End ghost being scared altogether
        setTimeout(() => {
          ghost.scared = false;
        }, 5000);
      });
      score += 20;
      if (!scoreElement) {
        console.error("Score element is missing!");
        return;
      } else {
        scoreElement.innerHTML = score.toString();
      }
    }
  }

  // Lose game scenario (ghost & player collision)
  for (let ghostIndex = ghosts.length - 1; 0 <= ghostIndex; ghostIndex--) {
    const ghost = ghosts[ghostIndex];
    if (
      Math.hypot(
        ghost.position.x - player.position.x,
        ghost.position.y - player.position.y
      ) <
      ghost.radius + player.radius
    ) {
      if (ghost.scared) {
        ghosts.splice(ghostIndex, 1);
      } else {
        let loseGameSound = new Sound({ src: "./src/audio/death.mp3" });
        loseGameSound.play();
        cancelAnimationFrame(animationId);
      }
    }
  }

  // Win condition
  if (pellets.length === 0) {
    console.log("You win");
    cancelAnimationFrame(animationId);
  }

  // Detect player / pellet collision
  for (let pelletIndex = pellets.length - 1; 0 <= pelletIndex; pelletIndex--) {
    const pellet = pellets[pelletIndex];

    pellet.draw();

    if (
      Math.hypot(
        pellet.position.x - player.position.x,
        pellet.position.y - player.position.y
      ) <
      pellet.radius + player.radius
    ) {
      pellets.splice(pelletIndex, 1);
      score += 10;
      eatPelletSound.play();
      if (!scoreElement) {
        console.error("Score element is missing!");
        return;
      } else {
        scoreElement.innerHTML = score.toString();
      }
    }
  }

  // Detect player / boundary collision
  boundaries.forEach((boundary) => {
    boundary.draw();

    // collision detector
    if (circleCollidesWithRectangle({ circle: player, rectangle: boundary })) {
      player.velocity.x = 0;
      player.velocity.y = 0;
    }
  });

  player.draw();
  if (!paused) {
    player.update();
  }
  // player.update();
  ghosts.forEach((ghost) => {
    ghost.draw();
    if (!paused) {
      ghost.update();
    }
    const collisions: CollisionType[] = [];

    boundaries.forEach((boundary) => {
      // Test if our ghost will collide to the top
      if (
        !collisions.includes("top") &&
        circleCollidesWithRectangle({
          circle: {
            ...ghost,
            velocity: {
              x: 0,
              y: -ghost.speed,
            },
          },
          rectangle: boundary,
        })
      ) {
        collisions.push("top");
      }
      // Test if our ghost will collide to the right
      if (
        !collisions.includes("right") &&
        circleCollidesWithRectangle({
          circle: {
            ...ghost,
            velocity: {
              x: ghost.speed,
              y: 0,
            },
          },
          rectangle: boundary,
        })
      ) {
        collisions.push("right");
      }

      // Test if our ghost will collide to the bottom
      if (
        !collisions.includes("bottom") &&
        circleCollidesWithRectangle({
          circle: {
            ...ghost,
            velocity: {
              x: 0,
              y: ghost.speed,
            },
          },
          rectangle: boundary,
        })
      ) {
        collisions.push("bottom");
      }

      // Test if our ghost will collide to the left
      if (
        !collisions.includes("left") &&
        circleCollidesWithRectangle({
          circle: {
            ...ghost,
            velocity: {
              x: -ghost.speed,
              y: 0,
            },
          },
          rectangle: boundary,
        })
      ) {
        collisions.push("left");
      }
    });
    if (collisions.length > ghost.prevCollisions.length) {
      ghost.prevCollisions = collisions;
    }

    if (JSON.stringify(collisions) !== JSON.stringify(ghost.prevCollisions)) {
      if (ghost.velocity.y < 0) {
        ghost.prevCollisions.push("top");
      } else if (ghost.velocity.x > 0) {
        ghost.prevCollisions.push("right");
      } else if (ghost.velocity.y > 0) {
        ghost.prevCollisions.push("bottom");
      } else if (ghost.velocity.x < 0) {
        ghost.prevCollisions.push("left");
      }

      const pathways = ghost.prevCollisions.filter((collision) => {
        return !collisions.includes(collision);
      });

      const direction = pathways[Math.floor(Math.random() * pathways.length)];

      switch (direction) {
        case "top":
          ghost.velocity.y = -ghost.speed;
          ghost.velocity.x = 0;
          break;

        case "right":
          ghost.velocity.y = 0;
          ghost.velocity.x = ghost.speed;
          break;

        case "bottom":
          ghost.velocity.y = ghost.speed;
          ghost.velocity.x = 0;
          break;

        case "left":
          ghost.velocity.y = 0;
          ghost.velocity.x = -ghost.speed;
          break;
      }

      ghost.prevCollisions = [];
    }
  });
  if (player.velocity.x > 0) {
    player.rotation = 0;
  } else if (player.velocity.x < 0) {
    player.rotation = Math.PI;
  } else if (player.velocity.y > 0) {
    player.rotation = Math.PI / 2;
  } else if (player.velocity.y < 0) {
    player.rotation = Math.PI * 1.5;
  }
  // The commented code below allows the
  // player to stop but it breaks turning
  // player.velocity.y = 0;
  // player.velocity.x = 0;
}