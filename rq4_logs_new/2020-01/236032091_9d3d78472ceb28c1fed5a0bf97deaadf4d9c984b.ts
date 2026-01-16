import WifiConfigurationService, { WifiConfigurationStatus } from './WifiConfigurationService'
import WifiScannerService, { WifiScanningStatus } from './WifiScannerService'


export default class GatewayDevice {

    private _device: BluetoothDevice
    private _server: BluetoothRemoteGATTServer | undefined

    configurationService: WifiConfigurationService | undefined
    scannerService: WifiScannerService | undefined



    constructor(device: BluetoothDevice) {
        this._device = device
    }

    async initialiseGattServer() {
        console.log('Initialising GATT Server')
        this._server = await this._device.gatt;
        return await this._device.gatt?.connect();
    }

    async initialiseServices(configurationServiceStatusChangeCallback: Function,
        scanningServiceStatusChangeCallback: Function) {
        console.log('Initialising Services')
        if (!this._server)
            return
        this.configurationService = new WifiConfigurationService(this._server, configurationServiceStatusChangeCallback)
        this.scannerService = new WifiScannerService(this._server, scanningServiceStatusChangeCallback)
        console.log('Device Services initing...')
        await Promise.all([
            this.configurationService.init(),
            this.scannerService.init()
        ])
        console.log('Device Services init')
        return
    }

    async getScanList(): Promise<void> {
        console.log('getScanList')
        if (!this.scannerService)
            throw Error( 'Scanner Service Not init' )
        await this.scannerService?.status?.setStatus( WifiScanningStatus.SCAN )
        return
    }

    async readScanList(): Promise<string[]> {
        console.log('readScanList')
        let list
        if (this.scannerService?.list) {
            list = await this.scannerService.list.getList1()
            if (list.length === 200)
                console.warn('List may be getting chopped off')
            // list = await this.scannerService.list.getList2()
            return JSON.parse(list)
        }
        return []
    }

    async pushCredentials(ssid: string, password: string, security: number): Promise<void> {
        console.log('pushCredentials', ssid, password, security)
        if (!this.configurationService)
            throw Error('Configuration Service not init')
        let status = await this.configurationService.status?.getStatus()
        if (status === WifiConfigurationStatus.IDLE) {
            console.log('config service idle. let\'s send stuff')
            await this.configurationService.ssid?.setSsid(ssid)
            await this.configurationService.password?.setPassword(password)
            await this.configurationService.security?.setSecurity(security)
            console.log('credentials sent')
        }
        return
    }

    async storeCredentials() {
        console.log('storeCredentials')
        if (!this.configurationService)
            throw Error('Configuration Service not init')
        let status = await this.configurationService.status?.getStatus()
        if (status === WifiConfigurationStatus.IDLE) {
            console.log('config service idle. let\'s store stuff')
            await this.configurationService.status?.setStatus(WifiConfigurationStatus.SAVE)
            console.log('credentials store command sent')
        }
        return
    }

    async joinNetwork() {
        console.log('joinNetwork')
        if (!this.configurationService)
            throw Error('Configuration Service not init')
        console.log('config saved already, let\'s connect to this network')
        await this.configurationService.status?.setStatus(WifiConfigurationStatus.JOIN)
        console.log('wifi connect command sent')
    }

    static async searchDevice(): Promise<GatewayDevice> {
        console.log('Searching Devices')
        return new GatewayDevice(await navigator.bluetooth.requestDevice({
            acceptAllDevices: true,
            optionalServices: [0x180D, WifiConfigurationService.uuid, WifiScannerService.uuid]
        }))
    }

}