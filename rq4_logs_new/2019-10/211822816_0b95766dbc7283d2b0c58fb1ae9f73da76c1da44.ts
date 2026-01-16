import { ActorAdapter } from "../adapter/actor";
import { StreamMeta } from "../entity/data/meta";
import { testSetupNodes } from "./setupnetwork";

describe("stream", () => {
  test("find", async () => {
    const num = 10;
    const nodes = await testSetupNodes(num);
    const actors = nodes.map(node => new ActorAdapter(node));

    let count = 0;
    const { url, event } = await actors[0].seeder.storeStream(
      "test",
      Buffer.from(`hello${count++}`)
    );
    const interval = setInterval(() => {
      event.execute(Buffer.from(`hello${count++}`));
      if (count > 10) clearInterval(interval);
    }, 1);

    const res = await new Promise<boolean>(async r => {
      const res = await actors[num - 1].user.connectSubNet(url);
      if (!res) {
        r(false);
        return;
      }
      const { subNet, meta } = res;
      let count = 0;
      subNet.findStreamMetaTarget(meta as StreamMeta, ab => {
        if (!ab) {
          r(true);
          return;
        }
        const receive = Buffer.from(ab).toString();
        const target = Buffer.from(`hello${count++}`).toString();
        expect(receive).toEqual(target);
      });
    });
    expect(res).toBe(true);
  }, 60_000_0);
});