import { PeerMockModule, PeerModule } from "../../vendor/kademlia";

import { PeerCreater } from "../../sp2p/module/peerCreater";
import { SP2P } from "../../sp2p/adapter/actor";
import { testSetupNodes } from "../setupnetwork";

describe("domain/user-navigator", () => {
  test("", async () => {
    const nodes = await testSetupNodes(8, PeerMockModule, {});
    const actors = nodes.map(
      v => new SP2P({ PeerCreater: new PeerCreater(PeerMockModule) }, v)
    );
    const seeder = actors.shift()!;
    const { url } = await seeder.seeder.storeStatic(
      "test",
      Buffer.from("test")
    );

    const user = actors.shift()!;
    await user.user.connectSubNet(url);

    expect(
      Object.keys(user.services.SeederManager.list[url].navigators).length > 0
    ).toBe(true);

    // seeder.dispose();

    // await new Promise(r => setTimeout(r, 3000));

    // const { subNet } = await actors[0].user.connectSubNet(url);
    // const ab = await subNet.findStaticMetaTarget();

    // expect(Buffer.from(ab!)).toEqual(Buffer.from("test"));
  }, 600_000);
});