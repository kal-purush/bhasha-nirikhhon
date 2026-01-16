import { defineConfig } from '../defineConfig';

export function createProductionConfig() {
  return defineConfig({
    env: 'production',
    apiURL: process.env.API_URL as string,
    port: Number(process.env.PORT) || 8000,
    databaseURL: process.env.DATABASE_URL as string,
    apiGeminiKey: process.env.API_GEMINI_KEY as string,
    jwtSecret: process.env.JWT_SECRET as string,
    saltRounds: Number(process.env.SALT_ROUNDS) || 10,
    mocksEnabled: process.env.ENABLE_MOCKS === 'true',
  });
}