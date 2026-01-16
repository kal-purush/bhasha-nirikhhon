import Kademlia, { PeerMockModule, PeerModule } from "../../vendor/kademlia";

import { SP2P } from "../../adapter/actor";
import { StaticMeta } from "../../entity/data/meta";
import { testSetupNodes } from "../setupnetwork";

describe("find", () => {
  const job = async (nodes: Kademlia[]) => {
    const actors = nodes.map(node => new SP2P(node));

    const actorStore = actors.pop()!;

    const { url } = await actorStore.seeder.storeStatic(
      "test",
      Buffer.from("hello")
    );

    await new Promise(r => setTimeout(r));

    for (let actor of actors) {
      const res = await actor.user.connectSubNet(url);

      expect(res).not.toBeUndefined();
      const { subNet, meta } = res!;

      const ab = await subNet.findStaticMetaTarget(meta as StaticMeta);
      expect(Buffer.from(ab!)).toEqual(Buffer.from("hello"));
      await new Promise(r => setTimeout(r));
    }
  };

  test("mock", async () => {
    const num = 10;
    const nodes = await testSetupNodes(num, PeerMockModule, { timeout: 1000 });
    await job(nodes);
  }, 60_000_0);

  test("webrtc", async () => {
    const num = 10;
    // timeout 1秒だとコケる
    const nodes = await testSetupNodes(num, PeerModule, { timeout: 5_000 });
    await job(nodes);
  }, 60_000_0);
});