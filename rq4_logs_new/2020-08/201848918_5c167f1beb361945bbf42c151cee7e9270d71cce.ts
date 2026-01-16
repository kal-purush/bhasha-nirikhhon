
import Converter, { fillLength } from './converter'
import TypeHints from './typeHints'
import Suiter, { SuitStatus, SuitTypes } from '../utils/suiter';
/**
 * @class
 * @classdesc 状态描述器<br>
 *
 * <pre>
 * 命名规则： get[设备类型名称]StatusDescriptor； 设备类型名称与SuiterMap配置表的key字段相同，<br>
 * 如led --> getLedStatusDescriptor<br>
 * </pre>
 */
class Descriptor {
  public readonly Suiter = {};
  public readonly SuitStatus = {};
  public readonly SuitTypes = {};
  public readonly TypeHints = {};
  public readonly Converter = {};
  constructor() {
    this.Suiter = Suiter;
    this.SuitStatus = SuitStatus;
    this.SuitTypes = SuitTypes;
    this.TypeHints = TypeHints;
    this.Converter = Converter;
  }

  getEquipTypeCode(deviceType:string, deviceChildType?:string):string{
    if (!deviceType) {
      console.warn('device type can not be empty!');
      return '';
    }
    return deviceChildType ? `${deviceType}${deviceChildType}` : `${deviceType}`
  }
  getEquipTypeDescriptor(deviceType:string, deviceChildType:string): string {
    const type = this.getEquipTypeCode(deviceType, deviceChildType)
    return (this.SuitTypes as any)[type];
  }

  public getPrimaryStatusCode(deviceType: string, status: string): string {
    if (!deviceType || !status) {
      console.warn('primary device type or status can not be empty!');
      return '';
    }
    return `${deviceType}${status}`;
  }
  public getSecondaryStatusCode(
    deviceType: string,
    deviceChildType: string,
    status: string
  ): string {
    if (!deviceType || !deviceChildType || !status) {
      console.warn('device type or status can not be empty!');
      return '';
    }
    return `${deviceType}${deviceChildType}${status}`;
  }
  public getDescriptorByCode(code: string): string {
    if (!code) { console.warn('key code can not be empty!'); }
    return (this.SuitStatus as any)[code];
  }

  public getMainDescriptor(deviceType: string, code: string): string {
    return this.getDescriptorByCode(
      this.getPrimaryStatusCode(deviceType, code)
    );
  }
  public getDescriptors(
    deviceType: string,
    statusBitStr: string,
    separator: string = ','
  ): string {
    let descriptor: Array<any> = [];
    for (let i = statusBitStr.length; i > 0; i -= 2) {
      const statusBit = statusBitStr.slice(i - 2, i);
      descriptor.push(
        this.getDescriptorByCode(
          this.getPrimaryStatusCode(deviceType, statusBit)
        )
      );
    }
    return descriptor.join(separator);
  }
  getSwitchDescriptor (status:string, deviceType:string, deviceChildType?:string): string {
    const socketStatus = new SocketStatus(status)
    if(!deviceChildType) return this.getMainDescriptor(deviceType, socketStatus.getState())

    const TypeHints = this.TypeHints as any
    const bitlen = TypeHints.getSocketSwitchLen(deviceChildType)

    if (TypeHints.isPlugSocketSwitch(deviceChildType)) {
      const statusBitStr = fillLength(socketStatus.getPlugStatus(), bitlen)
      return this.getDescriptors(deviceType, statusBitStr)
    }
    if (TypeHints.isTouchSocketSwitch(deviceChildType)) {
      const statusBitStr = fillLength(socketStatus.getTouchStatus(), bitlen)
      return this.getDescriptors(deviceType, statusBitStr)
    }
    if (TypeHints.isNormalSocketSwitch(deviceChildType)) {
      const statusBitStr = fillLength(socketStatus.getTouchStatus(), bitlen)
      return this.getDescriptors(deviceType, statusBitStr)
    }
    if (TypeHints.isMixSocketSwitch(deviceChildType)) {
      const statusBitStr = fillLength(socketStatus.getMixupStatus(), bitlen)
      return this.getDescriptors(deviceType, statusBitStr)
    }
    if (TypeHints.isSceneSocketSwitch(deviceChildType)) {
      const statusBitStr = fillLength(socketStatus.getSceneStatus(), bitlen)
      return this.getDescriptors(deviceType, statusBitStr)
    }
    return ''
  }
  getLampDescriptor (status:string, deviceType:string, deviceChildType:string) {
    const lampStatus = new LampStatus(status)
    // if (TypeHints.isSimpleLed())
  }
}