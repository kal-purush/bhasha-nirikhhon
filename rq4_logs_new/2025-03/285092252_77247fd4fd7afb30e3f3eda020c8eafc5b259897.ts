import { tool } from "opencontrol/tool";
import { client } from "../aws/client.js";
import { Resource } from "../resource.js";

/**
 * Creates an OpenControl tool that lists the resources in the current SST app. You can add
 * this tool to your OpenControl app by passing it to the `tools` option when creating an
 * OpenControl app.
 *
 * @example
 * ```js title="src/app.ts"
 * import { create } from "opencontrol";
 * import { tools } from "sst/opencontrol";
 *
 * const app = create({
 *   key: process.env.OPENCONTROL_KEY,
 *   tools: [...tools],
 * });
 * ```
 */
export const tools = [
  tool({
    name: "sst",
    description: "Get the resources in the current SST app",
    async run() {
      const c = await client();
      const stateBucket = await getStateBucket();
      if (!stateBucket)
        return {
          error:
            "Failed to find the SST state bucket in user's AWS account. Ask the user to make sure the AWS account has been bootstrapped with SST.",
        };

      const state = await getStateFile();
      if (!state)
        return {
          error: "Failed to find the SST state file in user's AWS account.",
        };

      const resources = state["checkpoint"]["latest"]["resources"];
      return resources
        .filter(
          (r: any) =>
            r.type !== "sst:sst:LinkRef" &&
            !r.type.startsWith("pulumi:provider:")
        )
        .map((r: any) => ({
          urn: r.urn,
          type: r.type,
          id: r.id,
          parent: r.parent,
        }));

      async function getStateBucket() {
        const res = await c.fetch(`https://ssm.${c.region}.amazonaws.com/`, {
          method: "POST",
          headers: {
            "X-Amz-Target": "AmazonSSM.GetParameter",
            "Content-Type": "application/x-amz-json-1.1",
          },
          body: JSON.stringify({
            Name: "/sst/bootstrap",
          }),
        });
        if (!res.ok) return;

        const data = (await res.json()) as {
          Parameter: {
            Value: string;
          };
        };
        return JSON.parse(data.Parameter.Value)["state"];
      }

      async function getStateFile() {
        const res = await c.fetch(
          `https://${stateBucket}.s3.${c.region}.amazonaws.com/app/${Resource.App.name}/${Resource.App.stage}.json`,
          {
            method: "GET",
          }
        );
        if (!res.ok) return;
        return (await res.json()) as any;
      }
    },
  }),
];