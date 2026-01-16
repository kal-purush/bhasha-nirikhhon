/**
 * @file This file contains code to make output .d.ts files NOT to have the line: /// <reference types="node" />
 */

declare global {
  interface URLSearchParams {}
}

// Without this we will get error:
// Augmentations for the global scope can only be directly nested in external modules or ambient module declarations.
export {};