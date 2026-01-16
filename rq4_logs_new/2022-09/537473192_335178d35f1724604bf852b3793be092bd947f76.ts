import Surreal from "surrealdb.js";
import withDocker from "./withDockerConnection";

const HumanReadableHost = async (db:Surreal, id:string) => {
  const {host, data} = await withDocker(db, id)
  const hostInfo = await host.info();
  return{
    ...data,
    containers: {
      total: hostInfo.Containers,
      running: hostInfo.ContainersRunning,
      paused: hostInfo.ContainersPaused,
      stopped: hostInfo.ContainersStopped
    },
    images: hostInfo.Images,
    system:{
      operatingSystem: hostInfo.OperatingSystem,
      type: hostInfo.OSType,
      name: hostInfo.Name
    }
  }
}

export default HumanReadableHost