import 'reflect-metadata';

@controller
class Plane {
  color: string = 'red';

  @markFunction('Hi There')
  fly(): void {
    console.log('Vrrrrrrrr');
  }
}

function markFunction(secret: string) {
  return (target: Plane, key: string) => {
    Reflect.defineMetadata('secret', secret, target, key);
  };
}

function controller(target: typeof Plane) {
  for (let key in target.prototype) {
    const secret = Reflect.getMetadata('secret', target.prototype, key);

    console.log('secret: ', secret);
  }
}

