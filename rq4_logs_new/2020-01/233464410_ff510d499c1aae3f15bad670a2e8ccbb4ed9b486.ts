import { MaskDDMMYYYDirective } from './mask-ddmmyyy.directive';

describe('MaskDDMMYYYDirective', () => {

  it('no key pressed', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('', 0, '');
    expect(newState).toEqual({string: '', position: 0});
  });
  it('1 pressed', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('', 0, '1');
    expect(newState).toEqual({string: '1', position: 1});
  });
  it('key pressed not digit nor /', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('', 0, 'd');
    expect(newState).toEqual({string: '', position: 0});
  });
  it('digit pressed with existing string equals to 1, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('1', 1, '1');
    expect(newState).toEqual({string: '11/', position: 3});
  });
  it('/ pressed with existing string equals to 1, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('1', 1, '/');
    expect(newState).toEqual({string: '01/', position: 3});
  });
  it('digit pressed with existing string equals to 1, cursor at 0', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('1', 0, '0');
    expect(newState).toEqual({string: '01/', position: 1});
  });
  it('digit pressed with existing string equals to 15/, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/', 3, '1');
    expect(newState).toEqual({string: '15/1', position: 4});
  });
  it('/ pressed with existing string equals to 15, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15', 2, '/');
    expect(newState).toEqual({string: '15/', position: 3});
  });
  it('1 pressed with existing string equals to 15/, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/', 3, '1');
    expect(newState).toEqual({string: '15/1', position: 4});
  });
  it('/ pressed with existing string equals to 15/, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/', 3, '/');
    expect(newState).toEqual({string: '15/', position: 3});
  });

  it('3 pressed with existing string equals to 15/, cursor at the start', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/', 0, '3');
    expect(newState).toEqual({string: '35/', position: 1});
  });

  it('2 pressed with existing string equals to 15/1, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/1', 4, '2');
    expect(newState).toEqual({string: '15/12/', position: 6});
  });
  it('2 pressed with existing string equals to 15/1, cursor at the end - 1', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/1', 3, '2');
    expect(newState).toEqual({string: '15/21/', position: 4});
  });
  it('1 pressed with existing string equals to 15/12, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/12', 5, '1');
    expect(newState).toEqual({string: '15/12', position: 5});
  });

  it('1 pressed with existing string equals to 11/02/, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('11/02/', 3, '1');
    expect(newState).toEqual({string: '11/12/', position: 4});
  });


  it('1 pressed with existing string equals to 15/12/, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/12/', 6, '1');
    expect(newState).toEqual({string: '15/12/1', position: 7});
  });
  it('0 pressed with existing string equals to 15/12/, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/12/', 6, '0');
    expect(newState).toEqual({string: '15/12/', position: 6});
  });
  it('2 pressed with existing string equals to 15/12/1, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/12/1', 7, '2');
    expect(newState).toEqual({string: '15/12/12', position: 8});
  });
  it('2 pressed with existing string equals to 15/12/1, cursor at the end - 1', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/12/1', 6, '2');
    expect(newState).toEqual({string: '15/12/21', position: 7});
  });
  it('/ pressed with existing string equals to 15/12/1, cursor at the end - 1', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/12/1', 6, '/');
    expect(newState).toEqual({string: '15/12/1', position: 6});
  });
  it('3 pressed with existing string equals to 15/12/12, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/12/12', 8, '3');
    expect(newState).toEqual({string: '15/12/123', position: 9});
  });
  it('4 pressed with existing string equals to 15/12/123, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/12/123', 9, '4');
    expect(newState).toEqual({string: '15/12/1234', position: 10});
  });
  it('5 pressed with existing string equals to 15/12/1234, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/12/1234', 10, '5');
    expect(newState).toEqual({string: '15/12/1234', position: 10});
  });
  it('5 pressed with existing string equals to 15/12/1234, cursor at the end - 1', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('15/12/1234', 9, '5');
    expect(newState).toEqual({string: '15/12/1235', position: 10});
  });

  it('0 pressed with existing string equals to 0, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('0', 1, '0');
    expect(newState).toEqual({string: '0', position: 1});
  });
  it('0 pressed with existing string equals to 12/0, cursor at the end', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('12/0', 4, '0');
    expect(newState).toEqual({string: '12/0', position: 4});
  });

  it('0 pressed with existing string equals to 01, cursor at the end - 1', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('01', 1, '0');
    expect(newState).toEqual({string: '01', position: 1});
  });
  it('0 pressed with existing string equals to 01/0, cursor at the end - 1', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('01/0', 4, '0');
    expect(newState).toEqual({string: '01/0', position: 4});
  });

  it('0 pressed with existing string equals to 01/01/2020, cursor at 1', () => {
    const newState = MaskDDMMYYYDirective.computeNewState('01/01/2020', 4, '0');
    expect(newState).toEqual({string: '01/01/2020', position: 4});
  });

});