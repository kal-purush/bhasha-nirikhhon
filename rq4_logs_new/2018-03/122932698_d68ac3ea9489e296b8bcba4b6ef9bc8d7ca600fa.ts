import { TestBed, inject } from '@angular/core/testing';

import { PicasaMapperService } from './picasa-mapper.service';

import * as picasa_album from './test/picasa-album-response.json';

describe('PicasaMapperService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [PicasaMapperService]
    });
  });

  it(
    'should be created',
    inject([PicasaMapperService], (service: PicasaMapperService) => {
      expect(service).toBeTruthy();
    })
  );

  it(
    'should parse photo album',
    inject([PicasaMapperService], (service: PicasaMapperService) => {
      debugger;

      const loadedAlbum = picasa_album as any;
      const album = service.parsePhotoAlbum(loadedAlbum.feed);
      expect(album).toBeTruthy();
    })
  );
});