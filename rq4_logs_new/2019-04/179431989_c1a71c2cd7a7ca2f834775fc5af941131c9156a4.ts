import dotenvExpand from 'dotenv-expand';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';

const defaults = ['.env.local', '.env'];

export function loadEnv(envFile?: string) {
  const files = envFile ? [envFile, ...defaults] : defaults;

  files.forEach(file => {
    const filePath = path.join(__dirname, '../../', file);
    if (fs.existsSync(filePath)) {
      console.log(`- Loading ${file}`);
      dotenvExpand(
        dotenv.config({
          path: filePath,
        })
      );
    }
  });
}