import { MarsPhoto } from './marsPhoto';

describe('Given the class MarsPhoto', () => {
  describe('When it new MarsPhoto is invoked to create a newMarsPhoto', () => {
    test('Then the class is instanced', () => {
      const newMarsPhoto = new MarsPhoto(
        '1',
        '2',
        '3',
        '4',
        '5',
        '6',
        '7',
        '8',
        '9',
        '10',
        '11',
        '12',
        '13',
        'APIPrivate',
        true,
        '16'
      );
      expect(newMarsPhoto).toBeInstanceOf(MarsPhoto);
    });
  });
});