declare module 'cryptographix-sim-core'
{
  import { Container, autoinject as inject } from 'aurelia-dependency-injection';

  export class Base64Codec {
      static decode(b64: string): Uint8Array;
      static encode(uint8: Uint8Array): string;
  }

  export class HexCodec {
      private static hexDecodeMap;
      static decode(a: string): Uint8Array;
  }

  export class ByteArray {
      static BYTES: number;
      static HEX: number;
      static BASE64: number;
      static UTF8: number;
      private byteArray;
      constructor(bytes?: ByteArray | Array<number> | String | ArrayBuffer | Uint8Array, format?: number, opt?: any);
      length: number;
      backingArray: Uint8Array;
      equals(value: ByteArray): boolean;
      byteAt(offset: number): number;
      wordAt(offset: number): number;
      littleEndianWordAt(offset: any): number;
      dwordAt(offset: number): number;
      setByteAt(offset: number, value: number): ByteArray;
      setBytesAt(offset: number, value: ByteArray): ByteArray;
      bytesAt(offset: number, count?: number): ByteArray;
      viewAt(offset: number, count?: number): ByteArray;
      addByte(value: number): ByteArray;
      setLength(len: number): ByteArray;
      concat(bytes: ByteArray): ByteArray;
      clone(): ByteArray;
      not(): ByteArray;
      and(value: ByteArray): ByteArray;
      or(value: ByteArray): ByteArray;
      xor(value: ByteArray): ByteArray;
      toString(format?: number, opt?: any): string;
  }

  export class KindHelper {
      private kindInfo;
      init(kindName: string, description: string): KindHelper;
      field(name: string, description: string, dataType: string, opts?: any): KindHelper;
      seal(kind?: Kind): KindInfo;
  }
  export class KindInfo {
      static $kindHelper: KindHelper;
      title: string;
      description: string;
      "type": string;
      properties: {};
  }
  export interface Kind {
      kindInfo: KindInfo;
      properties: {};
  }

  export class Key {
      protected id: string;
      protected cryptoKey: CryptoKey;
      constructor(id: string, key?: CryptoKey);
      type: string;
      algorithm: KeyAlgorithm;
      extractable: boolean;
      usages: string[];
      innerKey: CryptoKey;
  }


  export class PrivateKey extends Key {
  }


  export class PublicKey extends Key {
  }



  export class KeyPair {
      privateKey: PrivateKey;
      publicKey: PublicKey;
  }




  export class CryptographicService {
      protected crypto: SubtleCrypto;
      constructor();
      decrypt(algorithm: string | Algorithm, key: Key, data: ByteArray): Promise<ByteArray>;
      digest(algorithm: string | Algorithm, data: ByteArray): any;
      encrypt(algorithm: string | Algorithm, key: Key, data: ByteArray): Promise<ByteArray>;
      exportKey(format: string, key: Key): Promise<ByteArray>;
      generateKey(algorithm: string | Algorithm, extractable: boolean, keyUsages: string[]): Promise<Key | KeyPair>;
      importKey(format: string, keyData: ByteArray, algorithm: string | Algorithm, extractable: boolean, keyUsages: string[]): Promise<CryptoKey>;
      sign(algorithm: string | Algorithm, key: Key, data: ByteArray): Promise<ByteArray>;
      verify(algorithm: string | Algorithm, key: Key, signature: ByteArray, data: ByteArray): Promise<ByteArray>;
  }

  export { Container, inject };



  export interface MessageHeader {
      method?: string;
      id?: number;
      description?: string;
      isResponse?: boolean;
      origin?: EndPoint;
      kindName?: string;
  }
  export class Message<T> {
      private _header;
      private _payload;
      constructor(header: MessageHeader, payload: T);
      header: MessageHeader;
      payload: T;
  }
  export class KindMessage<K extends Kind> extends Message<K> {
  }

  export type Task = () => void;
  export type FlushFunc = () => void;
  export class TaskScheduler {
      static makeRequestFlushFromMutationObserver(flush: any): FlushFunc;
      static makeRequestFlushFromTimer(flush: any): FlushFunc;
      static BrowserMutationObserver: any;
      static hasSetImmediate: boolean;
      static taskQueueCapacity: number;
      taskQueue: Task[];
      requestFlushTaskQueue: FlushFunc;
      constructor();
      shutdown(): void;
      queueTask(task: any): void;
      flushTaskQueue(): void;
      onError(error: any, task: any): void;
  }



  export class Channel {
      private _active;
      private _endPoints;
      private _taskScheduler;
      constructor();
      shutdown(): void;
      active: boolean;
      activate(): void;
      deactivate(): void;
      addEndPoint(endPoint: EndPoint): void;
      removeEndPoint(endPoint: EndPoint): void;
      endPoints: EndPoint[];
      sendMessage(origin: EndPoint, message: Message<any>): void;
  }



  export enum Direction {
      IN = 1,
      OUT = 2,
      INOUT = 3,
  }
  export type HandleMessageDelegate = (message: Message<any>, receivingEndPoint?: EndPoint, receivingChannel?: Channel) => void;
  export class EndPoint {
      protected _id: string;
      protected _channels: Channel[];
      protected _messageListeners: HandleMessageDelegate[];
      private _direction;
      constructor(id: string, direction?: Direction);
      shutdown(): void;
      id: string;
      attach(channel: Channel): void;
      detach(channelToDetach: Channel): void;
      detachAll(): void;
      attached: boolean;
      direction: Direction;
      handleMessage(message: Message<any>, fromEndPoint: EndPoint, fromChannel: Channel): void;
      sendMessage(message: Message<any>): void;
      onMessage(messageListener: HandleMessageDelegate): void;
  }
  export type EndPointCollection = {
      [id: string]: EndPoint;
  };

  export enum ProtocolTypeBits {
      PACKET = 0,
      STREAM = 1,
      ONEWAY = 0,
      CLIENTSERVER = 4,
      PEER2PEER = 6,
      UNTYPED = 0,
      TYPED = 8,
  }
  export type ProtocolType = number;
  export class Protocol<T> {
      static protocolType: ProtocolType;
  }




  export class ComponentBuilder {
      private componentInfo;
      init(name: string, description: string): ComponentBuilder;
      port(id: string, direction: Direction, opts?: {
          protocol?: Protocol<any>;
          maxIndex?: number;
          required?: boolean;
      }): ComponentBuilder;
      install(ctor: ComponentConstructor): ComponentInfo;
  }
  export class PortInfo {
      direction: Direction;
      protocol: Protocol<any>;
      maxIndex: number;
      required: boolean;
  }
  export class ComponentInfo {
      static $builder: ComponentBuilder;
      name: string;
      description: string;
      ports: {
          [id: string]: PortInfo;
      };
      constructor();
  }
  export interface Component {
      componentInfo?: ComponentInfo;
      onCreate?(initialData: Kind): any;
      onDestroy?(): any;
      onStart?(endPoints: EndPointCollection): any;
      onStop?(): any;
  }
  export interface ComponentConstructor {
      new (...args: any[]): Component;
      componentInfo?: ComponentInfo;
  }




  export class ComponentContext {
      id: string;
      instance: Component;
      container: Container;
      factory: ComponentFactory;
      constructor(factory: ComponentFactory, id: string);
      componentLoaded(instance: Component): void;
      component: Component;
      load(): Promise<void>;
  }

  export class ModuleLoader {
      private moduleRegistry;
      constructor();
      private getOrCreateModuleRegistryEntry(address);
      loadModule(id: string): Promise<any>;
  }





  export class ComponentFactory {
      private loader;
      container: Container;
      constructor(loader: ModuleLoader, container: Container);
      createContext(id: string): ComponentContext;
      loadComponent(id: string): Promise<Component>;
      components: Map<string, ComponentConstructor>;
      get(id: string): ComponentConstructor;
      set(id: string, type: ComponentConstructor): void;
  }





  export class Port {
      protected _owner: Node;
      protected _protocolID: string;
      protected _endPoint: EndPoint;
      metadata: any;
      constructor(owner: Node, endPoint: EndPoint, attributes?: any);
      endPoint: EndPoint;
      toObject(opts?: any): Object;
      owner: Node;
      protocolID: string;
      id: string;
      direction: Direction;
  }
  export class PublicPort extends Port {
      proxyEndPoint: EndPoint;
      proxyChannel: Channel;
      constructor(owner: Graph, endPoint: EndPoint, attributes: {});
      connectPrivate(channel: Channel): void;
      disconnectPrivate(): void;
      toObject(opts?: any): Object;
  }





  export class Node {
      protected _owner: Graph;
      protected _id: string;
      protected _componentID: string;
      protected _initialData: Object;
      protected _ports: {
          [id: string]: Port;
      };
      metadata: any;
      context: ComponentContext;
      constructor(owner: Graph, attributes?: any);
      toObject(opts?: any): Object;
      owner: Graph;
      id: string;
      protected addPlaceholderPort(id: string, attributes: {}): Port;
      getPorts(): Port[];
      getPortByID(id: string): Port;
      identifyPort(id: string, protocolID?: string): Port;
      removePort(id: string): boolean;
      initComponent(factory: ComponentFactory): Promise<void>;
  }





  export type EndPointRef = {
      nodeID: string;
      portID: string;
  };
  export class Link {
      protected _owner: Graph;
      protected _id: string;
      protected _channel: Channel;
      protected _from: EndPointRef;
      protected _to: EndPointRef;
      protected _protocolID: string;
      protected metadata: any;
      constructor(owner: Graph, attributes?: any);
      toObject(opts?: any): Object;
      id: string;
      connect(channel: Channel): void;
      disconnect(): void;
      fromNode: Node;
      fromPort: Port;
      toNode: Node;
      toPort: Port;
      protocolID: string;
  }



  export class Network {
      private graph;
      private nodes;
      private links;
      private ports;
      private factory;
      constructor(graph: Graph, factory: ComponentFactory);
      initialize(): Promise<void>;
      protected initializeGraph(): Promise<void>;
      wireupGraph(router: any): void;
  }





  export class Graph extends Node {
      protected nodes: {
          [id: string]: Node;
      };
      protected links: {
          [id: string]: Link;
      };
      constructor(owner: Graph, attributes: any);
      toObject(opts: any): Object;
      initComponent(factory: ComponentFactory): Promise<void>;
      getNodes(): {
          [id: string]: Node;
      };
      getAllNodes(): Node[];
      getLinks(): {
          [id: string]: Link;
      };
      getAllLinks(): Link[];
      getAllPorts(): Port[];
      getNodeByID(id: string): Node;
      addNode(id: string, attributes: {}): Node;
      renameNode(id: string, newID: string): void;
      removeNode(id: string): boolean;
      getLinkByID(id: string): Link;
      addLink(id: string, attributes: {}): Link;
      renameLink(id: string, newID: string): void;
      removeLink(id: string): void;
      addPublicPort(id: string, attributes: {}): PublicPort;
  }




  export class SimulationEngine {
      loader: ModuleLoader;
      container: Container;
      constructor(loader: ModuleLoader, container: Container);
      getComponentFactory(): ComponentFactory;
  }
}