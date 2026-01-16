import {
  beforeAuthStateChanged as originalBeforeAuthStateChanged,
  onAuthStateChanged as originalOnAuthStateChanged
} from 'firebase/auth';

import * as authModule from './controllers';

describe('authModule', () => {
  it('reexports onAuthStateChanged from firebase/auth', () => {
    expect(authModule.onAuthStateChanged).toBe(originalOnAuthStateChanged);
  });

  it('reexports beforeAuthStateChanged from firebase/auth', () => {
    expect(authModule.beforeAuthStateChanged).toBe(
      originalBeforeAuthStateChanged
    );
  });
});