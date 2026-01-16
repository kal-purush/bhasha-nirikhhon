import { PeerModule } from "../../vendor/kademlia";
import { SP2P } from "../../adapter/actor";
import { testSetupNodes } from "../setupnetwork";

describe("static/subnet", () => {
  test("", async () => {
    const nodes = await testSetupNodes(4, PeerModule, { timeout: 15_000 });
    const actors = nodes.map(v => new SP2P(v));

    const seederNode = actors.shift()!;
    const { url } = await seederNode.seeder.storeStatic(
      "test",
      Buffer.from("test")
    );

    for (let actor of actors) {
      await actor.user.connectSubNet(url);
    }

    await new Promise(r => setTimeout(r, 60_000));

    expect(
      actors[0].services.SubNetworkManager.getSubNetwork(url).allPeers.length >
        1
    ).toBe(true);
  }, 600_000);
});