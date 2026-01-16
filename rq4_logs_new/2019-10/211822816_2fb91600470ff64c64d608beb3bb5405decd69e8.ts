import { Meta, meta2URL } from "../data/meta";
import {
  RPCCreatePeerAnswer,
  RPCCreatePeerOffer
} from "../../service/peer/createPeer";

import { EventManager } from "kad-rtc";
import { MainNetwork } from "../network/main";
import { Seeder } from "./seeder";

export class Navigator {
  constructor(
    private meta: Meta,
    mainNet: MainNetwork,
    private seeder: Seeder
  ) {
    this.listen(mainNet.kad.di.eventManager);
  }

  url = meta2URL(this.meta);

  listen(eventManager: EventManager) {
    const event = eventManager.selectListen<RPCCreatePeerOffer>(
      "RPCCreatePeerOffer"
    );
    event.subscribe(async ({ rpc, peer }) => {
      const { offer, id, url } = rpc;
      if (this.url === url) {
        const seederPeer = this.seeder.getPeers()[0];
        seederPeer.rpc(RPCNavigatorCallAnswer(offer, url, id));
        const { answer } = await peer
          .eventRpc<RPCCreatePeerAnswer>("RPCCreatePeerAnswer", id)
          .asPromise();
        peer.rpc(RPCCreatePeerAnswer(answer, id));
      }
    });
  }
}

const RPCNavigatorCallAnswer = (offer: string, url: string, id: string) => ({
  type: "RPCNavigatorCallAnswer" as const,
  url,
  offer,
  id
});

export type RPCNavigatorCallAnswer = ReturnType<typeof RPCNavigatorCallAnswer>;