import { SpotifyLinkPipe } from './spotify-link.pipe';

describe('SpotifyLinkPipe', () => {
  it('create an instance', () => {
    const pipe = new SpotifyLinkPipe();
    expect(pipe).toBeTruthy();
  });
});