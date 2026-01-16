/** @type {import('next').NextConfig} */
const withJest = require("next/jest");

const isTest = process.env.NODE_ENV === "test";

const nextConfig = {
  reactStrictMode: false,
  // other Next.js configurations
};

const jestConfig = {
  transform: {
    "^.+\\.(ts|tsx)$": ["babel-jest", { presets: ["next/babel"] }],
  },
};

module.exports = isTest ? withJest(jestConfig) : nextConfig;