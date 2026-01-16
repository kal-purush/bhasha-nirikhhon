class SocketStatus extends Status{
  plugStatus: string = ''
  touchStatus: string = ''
  mixupStatus: string = ''
  sceneStatus: string = ''
  constructor(status:string) {
    super(status)
    this.plugStatus = status.slice(0, 2)
    this.touchStatus = status.slice(0, 2)
    this.mixupStatus = status.slice(2, 4)
    this.sceneStatus = status.slice(6, 8)
  }
  getPlugStatus () {
    return this.plugStatus
  }
  getTouchStatus () {
    return this.touchStatus
  }
  getMixupStatus () {
    return this.mixupStatus
  }
  getSceneStatus () {
    return this.sceneStatus
  }
}