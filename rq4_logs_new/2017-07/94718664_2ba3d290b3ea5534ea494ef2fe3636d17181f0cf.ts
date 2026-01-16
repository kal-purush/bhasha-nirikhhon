import * as Discord from "discord.js";

import * as MissionModule from "./utils/mission";
import HandleMessage from "./utils/message_handler";



export default class ArmaBot extends Discord.Client {  
  public MissionList: Array<MissionModule.Mission>;



  constructor() { 
    super({
      messageCacheLifetime: 600
    }); 

    //*** Register event handlers
    this.on('error', console.error);
    this.on('warn', console.warn);
    this.on('debug', console.log);
    this.on('disconnect', () => { console.warn('*** Disconnected!'); });
    this.on('reconnecting', () => { console.warn('*** Reconnecting...'); });

    this.on('ready', () => {
      console.log(`Client ready; logged in as ${this.user.username}#${this.user.discriminator} (${this.user.id})`);
    });

    this.on("message", (message) => {
      HandleMessage(message);
    });
  }



  //*** Adds the new mission to the sv_MissionList array and returns a reference to the new mission
  createMission( a: MissionModule.ArgsCreateMission ) : MissionModule.Mission {
      let NewMission: MissionModule.Mission = {
          eventId: a.eventId,
          data: a.givenData,
          groups: a.givenGroups
      };
      this.MissionList.concat(NewMission);
      return NewMission;
  }

  //*** Deletes a mission from the cache based on the given Mission struct
  deleteMission( a_Mission: MissionModule.Mission ): void {}

  createMissionSlot(  ): void {}
};