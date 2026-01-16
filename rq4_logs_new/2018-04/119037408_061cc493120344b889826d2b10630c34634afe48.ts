import { ShallowRendererProps, CommonWrapper, EnzymeSelector, Intercepter } from 'enzyme';
import ComponentInstance from 'plusnew/dist/src/instances/types/Component/Instance';

abstract class PlusnewCommonWrapper {
  abstract wrapper: CommonWrapper


  filterWhere(predicate: (wrapper: this) => boolean) {
    this.wrapper.filterWhere(this.wrapper as any);
  };

  contains(node: plusnew.JSX.Element | plusnew.JSX.Element[] | string) {
    return this.wrapper.contains(node as any);
  }

  containsMatchingElement(node: plusnew.JSX.Element | plusnew.JSX.Element[]){
    return this.wrapper.containsMatchingElement(node as any);
  }

  containsAllMatchingElements(nodes: plusnew.JSX.Element[] | plusnew.JSX.Element[][] ){
    return this.wrapper.containsAllMatchingElements(nodes as any);
  }

  containsAnyMatchingElements(nodes: plusnew.JSX.Element[] | plusnew.JSX.Element[][]) {
    return this.wrapper.containsAnyMatchingElements(nodes as any);
  }

  equals(node: plusnew.JSX.Element) {
    return this.wrapper.equals(node as any);
  }

  matchesElement(node: plusnew.JSX.Element) {
    return this.wrapper.matchesElement(node as any);
  }


  hasClass(className: string) {
    return this.wrapper.hasClass(className);
  }

  is(selector: Component | string) {
    return this.wrapper.is(selector as any);
  }

  exists() {
    return this.wrapper.exists();
  };

  not(selector: Component | string) {
    return this.wrapper.not(selector);
  }

  text() {
    return this.wrapper.text();
  };

  html() {
    return this.wrapper.html();
  };

  get(index: number): plusnew.JSX.Element {
    return this.wrapper.get(index) as any;
  }

  getNode(): plusnew.JSX.Element {
    return this.wrapper.getNode() as any;
  }

  getNodes(): plusnew.JSX.Element[] {
    return this.wrapper.getNodes() as any;
  }

  getElement(): plusnew.JSX.Element {
    return this.wrapper.getElement() as any;
  }

  getElements(): plusnew.JSX.Element[] {
    return this.wrapper.getElement() as any;
  }

  getDOMNode(): Element {
    return this.wrapper.getDOMNode();
  }

  at(index: number): this{
    return this.wrapper.at(index) as any;
  }

  first(): this {
    return this.wrapper.first() as any;
  }

  last(): this {
    return this.wrapper.last() as any;

  }

  slice(begin?: number, end?: number): this {
    return this.wrapper.slice(begin, end) as any;
  }

  tap(intercepter: Intercepter<this>): this {
    return this.wrapper.tap(intercepter as any) as any;
  }

  state(key?: string) {
    if (arguments.length) {
      return this.wrapper.state();
    }
    return this.wrapper.state(key as any);
  }

  context() {
    throw new Error('Context does not exist');
  }


  props() {
    return this.wrapper.props();
  }

  prop(key: string) {
    return this.wrapper.prop(key);
  }

  key(): string {
    return this.wrapper.key();
  }

  simulate(event: string, ...args: any[]): this {
    return this.wrapper.simulate(event, ...args) as any;
  }


  setState(state: string, callback?: () => void) {
    return this.wrapper.setState(state, callback);
  }


  // setProps<K extends keyof P>(props: Pick<P, K>): this {

  // }

  // /**
  //  * A method that sets the context of the root component, and re-renders. Useful for when you are wanting to
  //  * test how the component behaves over time with changing contexts.
  //  * Returns itself.
  //  *
  //  * NOTE: can only be called on a wrapper instance that is also the root instance.
  //  */
  // setContext(context: any): this;

  // /**
  //  * Gets the instance of the component being rendered as the root node passed into shallow().
  //  *
  //  * NOTE: can only be called on a wrapper instance that is also the root instance.
  //  */
  // instance(): Component<P, S>;

  // /**
  //  * Forces a re-render. Useful to run before checking the render output if something external may be updating
  //  * the state of the component somewhere.
  //  * Returns itself.
  //  *
  //  * NOTE: can only be called on a wrapper instance that is also the root instance.
  //  */
  // update(): this;

  // /**
  //  * Returns an html-like string of the wrapper for debugging purposes. Useful to print out to the console when
  //  * tests are not passing when you expect them to.
  //  */
  // debug(): string;

  // /**
  //  * Returns the name of the current node of the wrapper.
  //  */
  // name(): string;

  // /**
  //  * Iterates through each node of the current wrapper and executes the provided function with a wrapper around
  //  * the corresponding node passed in as the first argument.
  //  *
  //  * Returns itself.
  //  * @param fn A callback to be run for every node in the collection. Should expect a ShallowWrapper as the first
  //  *              argument, and will be run with a context of the original instance.
  //  */
  // forEach(fn: (wrapper: this, index: number) => any): this;

  // /**
  //  * Maps the current array of nodes to another array. Each node is passed in as a ShallowWrapper to the map
  //  * function.
  //  * Returns an array of the returned values from the mapping function..
  //  * @param fn A mapping function to be run for every node in the collection, the results of which will be mapped
  //  *              to the returned array. Should expect a ShallowWrapper as the first argument, and will be run
  //  *              with a context of the original instance.
  //  */
  // map<V>(fn: (wrapper: this, index: number) => V): V[];

  // /**
  //  * Applies the provided reducing function to every node in the wrapper to reduce to a single value. Each node
  //  * is passed in as a ShallowWrapper, and is processed from left to right.
  //  */
  // reduce<R>(fn: (prevVal: R, wrapper: this, index: number) => R, initialValue?: R): R;

  // /**
  //  * Applies the provided reducing function to every node in the wrapper to reduce to a single value.
  //  * Each node is passed in as a ShallowWrapper, and is processed from right to left.
  //  */
  // reduceRight<R>(fn: (prevVal: R, wrapper: this, index: number) => R, initialValue?: R): R;

  // /**
  //  * Returns whether or not any of the nodes in the wrapper match the provided selector.
  //  */
  // some(component: Component | string): boolean;

  // /**
  //  * Returns whether or not any of the nodes in the wrapper pass the provided predicate function.
  //  */
  // someWhere(fn: (wrapper: this) => boolean): boolean;

  // /**
  //  * Returns whether or not all of the nodes in the wrapper match the provided selector.
  //  */
  // every(component: Component | string): boolean;

  // /**
  //  * Returns whether or not all of the nodes in the wrapper pass the provided predicate function.
  //  */
  // everyWhere(fn: (wrapper: this) => boolean): boolean;

  // /**
  //  * Returns true if renderer returned null
  //  */
  // isEmptyRender(): boolean;

  // /**
  //  * Renders the component to static markup and returns a Cheerio wrapper around the result.
  //  */
  // render(): Cheerio;

  // /**
  //  * Returns the type of the current node of this wrapper. If it's a composite component, this will be the
  //  * component constructor. If it's native DOM node, it will be a string of the tag name.
  //  *
  //  * Note: can only be called on a wrapper of a single node.
  //  */
  // type(): string | ComponentClass<P> | StatelessComponent<P>;

  // length: number;
}

type Component = (props: any, instance: ComponentInstance) => plusnew.JSX.Element | null;

export default PlusnewCommonWrapper;

export { Component };