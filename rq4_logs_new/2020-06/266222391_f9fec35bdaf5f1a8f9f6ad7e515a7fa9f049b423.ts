import Prototype, {
  Button,
  Input
} from './src/Prototype';
import PrototypeRegistry from './src/PrototypeRegistry';

(function main() {
  const prototypeRegistry = new PrototypeRegistry();
  const button = new Button("red");
  const input = new Input("Playing with prototype pattern")

  prototypeRegistry.addPrototypes([button, input]);

  function cloneThesePrototypes(prototypes: Prototype[]): Prototype[]{
    return prototypes.map(prototype => prototypeRegistry
      .getByIdentifier(prototype.getIdentifier()).clone())
  }
  const [clonedButton, clonedInput] = cloneThesePrototypes([button, input]);
  
  console.log(clonedButton, clonedInput)
})();