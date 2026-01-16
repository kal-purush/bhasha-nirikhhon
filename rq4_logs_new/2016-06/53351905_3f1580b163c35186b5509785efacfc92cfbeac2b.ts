export class LazyLoader {

  static lazyLoad(lazymodule:string):Promise<any> {

    var resolver = (resolve, reject) => {

      switch (lazymodule) {
        case 'About':
          require.ensure([], (require) => {
            resolve(require('../../../app/+about/index'));
          });
          break;
        case 'Playground':
          require.ensure([], (require) => {
            resolve(require('../../../app/+playground/index'));
          });
          break;
      }
    };

    return new Promise<any>(resolver);
  }
}