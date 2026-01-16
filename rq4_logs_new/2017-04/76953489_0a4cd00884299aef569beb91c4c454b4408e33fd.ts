import { async, fakeAsync, TestBed, inject } from '@angular/core/testing';
import { AvailabilitySvc } from '../availability.service';
import { DecHttp } from '../../../utils/http/';
import { CollectionObjectDiffer } from '../../../utils/differs/collection-object-diff';
import { ColObjDifferFactoryMock, DecHttpMock } from './availability.mocks';

describe("Availability Service", () => {

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        AvailabilitySvc,
        DecHttpMock,
        CollectionObjectDiffer
      ]
    });
  });

  it('initialises', inject([AvailabilitySvc], (availabilitySvc) => {
    expect(availabilitySvc).not.toBeNull();
	}));

  it("debouncedSync()", () => {

  });

});