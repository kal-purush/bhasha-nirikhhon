import React from 'react';

declare global {
  namespace JSX {
    interface IntrinsicElements {
      [elemName: string]: any;
    }
  }
}

// Declare module definitions for imports that TypeScript can't find
declare module 'next/link';
declare module 'next/image';
declare module 'lucide-react';