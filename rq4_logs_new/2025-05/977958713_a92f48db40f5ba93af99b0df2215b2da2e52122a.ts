'use client';

import { useEffect } from 'react';
import ColorThief from 'color-thief-browser';

function rgbToRgba(rgb: number[], alpha = 1) {
  return `rgba(${rgb[0]}, ${rgb[1]}, ${rgb[2]}, ${alpha})`;
}

export function useDynamicTheme(avatarUrl: string) {
  useEffect(() => {
    const img = new Image();
    img.crossOrigin = 'anonymous';
    img.src = avatarUrl;

    img.onload = () => {
      const thief = new ColorThief();
      const main = thief.getColor(img); // 主色
      const palette = thief.getPalette(img, 3); // 取更多颜色

      const [primary, secondary = main] = [main, palette[1]];
      const style = document.documentElement.style;

      console.log(primary);
      console.log(secondary);

      style.setProperty('--theme-primary', rgbToRgba(primary));
      style.setProperty('--theme-secondary', rgbToRgba(secondary));
    };
  }, [avatarUrl]);
}