import { TestBed, async } from '@angular/core/testing';

import { StoreService } from './store.service';

interface StoreState {
  firstName: string;
  lastName: string;
}

const initialState: StoreState = {
  firstName: 'John',
  lastName: 'Doe'
};

class StoreStateStub extends StoreService<StoreState> {
  constructor() {
    super(initialState);
  }
}

describe('StoreService', () => {
  let service: StoreService<StoreState>;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = new StoreStateStub();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should return initial state', () => {
    expect(service.state).toBe(initialState);
  });

  it('should update state', () => {
    const newState = { ...service.state, lastName: 'Smith' };
    service.setState(newState);
    expect(service.state).toBe(newState);
  });

  it('should notify subscriber', async(() => {
    const newState = { ...service.state, lastName: 'Smith' };
    let called = 0;

    service.state$.subscribe((state: StoreState) => {
      if (called === 0) {
        expect(state).toEqual(initialState);
      } else {
        expect(state).toEqual(newState);
      }
      called++;
    });
    expect(called).toBe(1);

    service.setState(newState);
    expect(called).toBe(2);
  }));

  it('should not notify subscriber after unsubscribe', async(() => {
    const newState = { ...service.state, lastName: 'Smith' };
    let called = 0;

    const subscription = service.state$.subscribe(() => {
      called++;
    });
    expect(called).toBe(1);

    subscription.unsubscribe();
    service.setState(newState);
    expect(called).toBe(1);
  }));
});