@classDecorator
class Boat {
  @testDecorator
  color: string = 'red';

  @testDecorator
  get formattedColor(): string {
    return `The Boat color is ${this.color}`;
  }

  @logError('Ooops, Boat is sunk.')
  pilot(): void {
    throw new Error();
    console.log('Swish');
  }

  @logError('Moving went through an error')
  move(
    @parameterDecorator speed: string,
    @parameterDecorator generateWeek: boolean
  ): void {
    if (speed === 'fast') {
      console.log('Moving super fast.....');
    } else {
      console.log('Moving nothing....');
    }
  }
}

function classDecorator(constructor: typeof Boat) {
  console.log('calss decorator: ', constructor);
}

function parameterDecorator(target: any, key: string, index: number) {
  console.log(
    'Parameter decoratotr targe: ',
    target,
    ' key: ',
    key,
    ' index: ',
    index
  );
}

function testDecorator(target: any, key: string): void {
  console.log('Target:', target);
  console.log('Key:', key);
}

function logError(errorMessage: string) {
  // decorator factory

  return (target: any, key: string, desc: PropertyDescriptor): void => {
    // decorator
    const { value: method } = desc;

    desc.value = () => {
      try {
        method();
      } catch (e) {
        console.log(errorMessage);
      }
    };
  };
}

new Boat().pilot();
new Boat().move('slow', true);