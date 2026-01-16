import { TestBed, async } from '@angular/core/testing';

import { StateService } from './state.service';

interface State {
  firstName: string;
  lastName: string;
}

const initialState: State = {
  firstName: 'John',
  lastName: 'Doe'
};

const newState: State = {
  firstName: 'John',
  lastName: 'Smith'
};

class StateStateStub extends StateService<State> {
  constructor() {
    super(initialState);
  }

  setLastName(lastName: string) {
    this.setState({ ...this.state, lastName: lastName });
  }
}

describe('StateService', () => {
  let service: StateStateStub;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = new StateStateStub();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should return initial state', () => {
    expect(service.state).toBe(initialState);
  });

  it('should update state', () => {
    service.setLastName(newState.lastName);
    expect(service.state).not.toBe(newState);
    expect(service.state).toEqual(newState);
  });

  it('should notify subscriber', async(() => {
    let called = 0;

    service.state$.subscribe((state: State) => {
      if (called === 0) {
        expect(state).toEqual(initialState);
      } else {
        expect(state).toEqual(newState);
      }
      called++;
    });
    expect(called).toBe(1);

    service.setLastName(newState.lastName);
    expect(called).toBe(2);
  }));

  it('should not notify subscriber after unsubscribe', async(() => {
    let called = 0;

    const subscription = service.state$.subscribe(() => {
      called++;
    });
    expect(called).toBe(1);

    subscription.unsubscribe();
    service.setLastName(newState.lastName);
    expect(called).toBe(1);
  }));
});