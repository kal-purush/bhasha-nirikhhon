import '@testing-library/jest-dom';
import { TextEncoder, TextDecoder } from 'util';

global.TextEncoder = TextEncoder;
(global as any).TextDecoder = TextDecoder;

// Configure console for better test logging
const originalConsoleLog = console.log;
const originalConsoleError = console.error;
const originalConsoleWarn = console.warn;

console.log = (...args) => {
  originalConsoleLog('[TEST]', ...args);
};

console.error = (...args) => {
  originalConsoleError('[TEST ERROR]', ...args);
};

console.warn = (...args) => {
  originalConsoleWarn('[TEST WARNING]', ...args);
};

// Mock localStorage
Object.defineProperty(window, 'localStorage', {
  value: {
    store: {} as Record<string, string>,
    getItem(key: string) {
      console.log(`[Storage] Getting item: ${key}`);
      return this.store[key] || null;
    },
    setItem(key: string, value: string) {
      console.log(`[Storage] Setting item: ${key}`);
      this.store[key] = value.toString();
    },
    removeItem(key: string) {
      console.log(`[Storage] Removing item: ${key}`);
      delete this.store[key];
    },
    clear() {
      console.log('[Storage] Clearing all items');
      this.store = {};
    }
  },
  writable: true
});

// Mock window.location
Object.defineProperty(window, 'location', {
  value: {
    href: 'http://localhost:3000',
    search: '',
    pathname: '/',
    origin: 'http://localhost:3000',
    protocol: 'http:',
    host: 'localhost:3000',
    hostname: 'localhost',
    port: '3000',
  },
  writable: true
});

// Reset mocks between tests
beforeEach(() => {
  window.localStorage.clear();
  console.log('[TEST] Environment reset complete');
});