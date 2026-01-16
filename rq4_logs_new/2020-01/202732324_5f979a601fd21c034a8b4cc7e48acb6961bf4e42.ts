import { CustomtextPipe } from './customtext.pipe';
import {CustomtextService} from "./customtext.service";

describe('CustomtextPipe', () => {
  it('create an instance of CustomtextService, transform via pipe, get valid texts', () => {
    const cts = new CustomtextService;
    cts.addCustomTexts({
          'ctv1': 'Sosososo',
          'ctv2': 'Düdüdüdü',
          'ctv3': 'yoyoyoyo'
        }
    );
    const pipe = new CustomtextPipe(cts);
    expect(pipe.transform('any', 'ctv1')).toBe('Sosososo');
    expect(pipe.transform('any', 'ctv2')).toBe('Düdüdüdü');
    expect(pipe.transform('any', 'ctv3')).toBe('yoyoyoyo');
    expect(pipe.transform('any', 'ctv4')).toBe('ctv4');
  });
});