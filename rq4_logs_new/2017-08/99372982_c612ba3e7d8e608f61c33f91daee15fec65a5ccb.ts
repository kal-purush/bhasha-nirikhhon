import {
    TestBed,
    async,
    ComponentFixture
} from '@angular/core/testing';
import {DebugElement} from '@angular/core';

import BaseTestingConfiguration from '../../spec/init';

import {Album} from '../../models/album';

import {AlbumComponent} from '../../components/album/index';

describe('AlbumComponent', () => {
  jasmine.DEFAULT_TIMEOUT_INTERVAL = 10000;

  let comp: AlbumComponent;
  let fixture: ComponentFixture<AlbumComponent>;
  let de: DebugElement;
  let el: HTMLElement;

  beforeEach(async(() => {
    TestBed.configureTestingModule(BaseTestingConfiguration).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AlbumComponent);
    comp = fixture.componentInstance;
    de = fixture.debugElement;
    el = de.nativeElement;
  });

  it('should create component', () => {
    expect(comp).toBeTruthy();
  });

  it(`should have init properties`, () => {
    expect(comp.preloader).toBeTrue();
    expect(comp.album).toBeUndefined();
  });

  it(`should have main properties`, (done) => {
    comp.ngOnInit().then(() => {
      expect(comp.preloader).toBeFalse();
      expect(comp.album instanceof Album).toBeTrue();
      done();
    }).catch(done);
  });

  it(`should render properly`, (done) => {
    comp.ngOnInit().then(() => {
      expect(el.querySelector('[data-unit-test="album-title"]').textContent).toContain(comp.album.title);
      expect(el.querySelectorAll('[data-unit-test="album-pictures"] img').length).toBeGreaterThan(0);
      expect(el.querySelector('[data-unit-test="album-back"]').textContent).toContain('Back');
      done();
    }).catch(done);
  });
});