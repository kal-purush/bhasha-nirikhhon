
type InjectInstance = any;

class Injectable {
  dependencies: Injectable[];

  instance: InjectInstance;
  resolver: any;

  constructor() {

  }

  resolve(): InjectInstance
  {
    this.dependencies.forEach( i => { return i.resolve(); } );

    return this.instance;
  }
}


/**
* A lightweight, extensible dependency injection container.
*/
export class Container {
  /**
  * The parent container in the DI hierarchy.
  */
  parent: Container;

  /**
  * The root container in the DI hierarchy.
  */
  root: Container;

  _resolvers;
  _configuration;

  /**
  * Creates an instance of Container.
  * @param configuration Provides some configuration for the new Container instance.
  */
  constructor(configuration?: {}) {
    if (!configuration) {
      configuration = {};
    }

    this._configuration = configuration;
    this._resolvers = new Map();
    this.root = this;
    this.parent = null;
  }

  /**
  * Creates a new dependency injection container whose parent is the current container.
  * @return Returns a new container instance parented to this.
  */
  createChild(): Container {
    let child = new Container(this._configuration);

    child.root = this.root;
    child.parent = this;

    return child;
  }

}