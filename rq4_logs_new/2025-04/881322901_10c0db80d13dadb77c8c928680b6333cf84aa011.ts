export type OctopusProxyServerProtocol = 'http';
export type OctopusProxyServerHost = string;
export type OctopusProxyServerPort = number;
export interface OctopusProxyServerInterface {
    protocol: OctopusProxyServerProtocol;
    host: OctopusProxyServerHost;
    port: OctopusProxyServerPort;
}
export interface OctopusProxyServerConstructorParamInterface {
    protocol?: OctopusProxyServerProtocol;
    host?: OctopusProxyServerHost;
    port?: OctopusProxyServerPort;
}