import { EndPointCollection } from '../messaging/end-point';
import { Node } from '../graph/node';
import { ComponentFactory} from './component-factory';
import { Component } from './component';

import { Container, Injectable } from '../dependency-injection/container';

export enum RunState {
  NOT_LOADED,   // No component loaded
  LOADING,      // Waiting for async load to complete
  LOADED,       // Component loaded, not yet executable
  READY,        // Ready for Execution
  RUNNING,      // Network active, and running
  PAUSED        // Network temporarily paused
}

/**
* The runtime context information for a Component instance
*/
export class RuntimeContext
{
  /**
  * The component id / address
  */
  private _id: string;

  /**
  * The runtime component instance that this node represents
  */
  private _instance: Component;

  /**
  * Initial Data for the component instance
  */
  private _initialData: {};

  /**
  * The runtime component instance that this node represents
  */
  private _container: Container;

  /**
  * The component factory that created us
  */
  private _factory: ComponentFactory;

  /**
  *
  *
  */
  constructor( factory: ComponentFactory, container: Container, id: string, initialData: {}, deps: Injectable[] = [] ) {

    this._factory = factory;

    this._id = id;

    this._initialData = initialData;

    this._container = container;

    // Register any context dependencies
    for( let i in deps )
    {
      if ( !this._container.hasResolver( deps[i] ) )
        this._container.registerSingleton( deps[i], deps[i] );
    }
  }

  get instance(): Component {
    return this._instance;
  }

  get container(): Container {
    return this._container;
  }

  load( ): Promise<void>
  {
    let me = this;

    this._instance = null;

    return new Promise<void>( (resolve, reject) => {
      // get an instance from the factory
      me._runState = RunState.LOADING;
      this._factory.loadComponent( this, this._id )
        .then( (instance) => {
          // Component (and any dependencies) have been loaded
          me._instance = instance;
          me._runState = RunState.LOADED;

          resolve();
        })
        .catch( (err) => {
          // Unable to load
          me._runState = RunState.NOT_LOADED;

          reject( err );
        });
    } );
  }

  _runState: RunState = RunState.NOT_LOADED;
  get runState() {
    return this._runState;
  }

  private inState( states: RunState[] ): boolean {
      return new Set<RunState>( states ).has( this._runState );
  }

  setRunState( runState: RunState ) {
    function callOptional<T>( fn: Function, ...args ): T
    {
      if ( fn )
        return fn( ...args );

      return null;
    }

    switch( runState )
    {
      case RunState.LOADED: // teardown node
        if ( this.inState( [ RunState.READY, RunState.RUNNING, RunState.PAUSED ] ) ) {
          callOptional( this.instance.teardown );
        }
        break;

      case RunState.READY:  // setup or stop node
        if ( this.inState( [ RunState.LOADED ] ) ) {
          let endPoints = callOptional<EndPointCollection>( this.instance.setup, this._initialData );

          this.reconcilePorts( endPoints );
        }
        else if ( this.inState( [ RunState.RUNNING, RunState.PAUSED ] ) ) {
          callOptional( this.instance.stop );
        }
        break;

      case RunState.RUNNING:  // start/resume node
        if ( this.inState( [ RunState.LOADED, RunState.RUNNING ] ) ) {
          callOptional( this.instance.start );
        }
        else if ( this.inState( [ RunState.PAUSED ] ) ) {
          callOptional( this.instance.resume );
        }
        else {
          throw new Error( 'Network cannot be started, not ready' );
        }
        break;

      case RunState.PAUSED:  // pause node
        if ( this.inState( [ RunState.RUNNING] ) ) {
          callOptional( this.instance.pause );
        }
        else if ( this.inState( [ RunState.PAUSED ] ) ) {
          // node.context.resume();
        }
        break;
    }
  }

  protected reconcilePorts( endPoints: EndPointCollection ) {
    //let ports = this.node.ports;
    //end
  }

  release() {
    // release instance, to avoid memory leaks
    this._instance = null;

    this._factory = null
  }
}