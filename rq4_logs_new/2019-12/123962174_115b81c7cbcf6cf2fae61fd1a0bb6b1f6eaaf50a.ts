import { Bot } from '../Bot';

export function drawBot({bot, ctx}: {bot: Bot, ctx: CanvasRenderingContext2D}) {
  ctx.lineWidth = 1;
  ctx.fillStyle = 'rgb(100,100,255)';
  ctx.strokeStyle = '#003300';

  ctx.beginPath();
  ctx.arc(bot.x, bot.y, bot.radius, 0, 2 * Math.PI, false);
  ctx.fill();
}