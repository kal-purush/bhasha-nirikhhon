import 'isomorphic-fetch';
import { SensorControl } from '../../src/controls/sensorcontrol';
import { DymoManager } from '../../src/manager';
import { forAll } from '../../src/generator/expression-generator';
import { DymoGenerator } from '../../src/generator/dymo-generator';
import * as Uri from '../../src/globals/uris';

// from semantic player generator
function addSensorSliderConstraint(
  name: string,
  sensorType: string,
  param: string,
  dymoGen: DymoGenerator
) {
  let slider = dymoGen.addControl(name, Uri.SLIDER);
  dymoGen.addConstraint(
    forAll("d").ofType(Uri.DYMO).forAll("c").in(slider).assert(param+"(d) == c"));
  let sensor = dymoGen.addControl(undefined, sensorType);
  dymoGen.addConstraint(
    forAll("d").ofType(Uri.DYMO).forAll("c").in(sensor).assert(param+"(d) == c"));
}

describe('SensorControl Integration (Manager downwards)', () => {
  it('Updates associated sliders on change', async () => {
    const manager = new DymoManager(null);
    const load = manager.init(
      'https://raw.githubusercontent.com/semantic-player/dymo-core/master/ontologies/'
    );

    await load;
    const store = manager.getStore();
    store.addDymo("dymo1");
    store.setParameter("dymo1", Uri.AMPLITUDE, 1);
    const generator = new DymoGenerator(store);
    addSensorSliderConstraint(
      'Amp',
      Uri.ACCELEROMETER_X,
      'Amplitude',
      generator
    );
    const stuff = await manager.loadIntoStore(); // is there any other way to get the store to load everything?
    const sensors = manager.getSensorControls();
    const sliders = manager.getUIControls();
    expect(sensors.length).toBe(1);
    expect(sliders.length).toBe(1);
    const accX = sensors[0];
    accX.startUpdate();
    accX.updateValue(10);
    expect(accX.getValue()).toBe(10);
    expect(store.findObjectValue(accX.getUri(), Uri.VALUE)).toBe(10);
    const slider = sliders[0];
    expect(store.getValueObservers(slider.getUri(), Uri.VALUE).length).toBe(2);
    const hmm = store.getValueObservers(slider.getUri(), Uri.VALUE);
    expect(store.getValueObservers(accX.getUri(), Uri.VALUE).length).toBe(2);
    expect(slider.getValue()).toBe(10);
    expect(store.findParameterValue('dymo1', Uri.AMPLITUDE)).toBe(10);
  });
});