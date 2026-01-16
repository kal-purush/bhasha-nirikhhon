/* eslint-disable */
// @ts-nocheck
import { NextApiRequest, NextApiResponse } from 'next';
import { createCanvas, registerFont, loadImage } from 'canvas';

import fs from 'fs';
import matter from 'gray-matter';

const createOgp = async (req: NextApiRequest, res: NextApiResponse): Promise<void> => {
  const slug = req.query.slug;
  const file = fs.readFileSync(`src/posts/${slug}.md`, 'utf-8');
  const { data } = matter(file);

  const WIDTH = 1200 as const;
  const HEIGHT = 630 as const;
  const DX = 0 as const;
  const DY = 0 as const;
  const canvas = createCanvas(WIDTH, HEIGHT);

  const context = canvas.getContext('2d');

  context.fillStyle = '#fff';
  context.fillRect(DX, DY, WIDTH, HEIGHT);

  const backgroundImage = await loadImage('public/images/ogp_background.png');
  context.drawImage(backgroundImage, DX, DY, WIDTH, HEIGHT);

  registerFont('src/assets/fonts/Roboto-Regular.ttf', { family: 'roboto' });
  context.font = 'bold 68px roboto';
  context.fillStyle = '#fff';
  context.textAlign = 'center';
  context.textBaseline = 'middle';
  context.fillText(data.title, 600, 300);

  context.fillStyle = '#aaa';
  context.font = '40px roboto';
  context.fillText(data.date.replace(/-/g, '/'), 1040, 580);

  context.font = '32px roboto';
  context.fillText("- Tsue's sandbox -", 600, 220);

  const buffer = canvas.toBuffer();

  res.writeHead(200, {
    'Content-Type': 'image/png',
    'Content-Length': buffer.length,
  });
  res.end(buffer, 'binary');
};

export default createOgp;