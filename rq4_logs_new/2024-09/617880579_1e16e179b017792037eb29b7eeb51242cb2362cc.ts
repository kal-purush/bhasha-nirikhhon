import yargs from "yargs";

import runWithPlatform from "../../lib/run-with-platform";
import { TdrivePlatform } from "../../../core/platform/platform";
import { DatabaseServiceAPI } from "../../../core/platform/services/database/api";
import {
  DriveFile,
  EditingSessionKeyFormat,
  TYPE,
} from "../../../services/documents/entities/drive-file";

async function report(platform: TdrivePlatform) {
  const drivesRepo = await platform
    .getProvider<DatabaseServiceAPI>("database")
    .getRepository<DriveFile>(TYPE, DriveFile);
  const editedFiled = (await drivesRepo.find({ editing_session_key: { $ne: null } })).getEntities();
  console.error("DriveFiles with non null editing_session_key (url encoded):");
  console.error("");
  editedFiled.forEach(dfile => {
    console.error(`- ${dfile.name} (${dfile.id}) has key:`);
    const parsed = EditingSessionKeyFormat.parse(dfile.editing_session_key);
    console.error(`    - URL encoded:   ${encodeURIComponent(dfile.editing_session_key)}`);
    console.error(`    - applicationId: ${parsed.applicationId}`);
    console.error(`    - companyId:     ${parsed.companyId}`);
    console.error(`    - instanceId:    ${parsed.instanceId}`);
    console.error(
      `    - userId:        ${parsed.userId} (${
        parsed.userId === dfile.creator ? "same as creator ID" : "not the creator"
      })`,
    );
    console.error(
      `    - timestamp:     ${parsed.timestamp.toISOString()} (${Math.floor(
        (new Date().getTime() - parsed.timestamp.getTime()) / 1000,
      )}s ago)`,
    );
  });
  if (!editedFiled.length) console.error("  (no DriveFile currently has an editing_session_key)");
}

const command: yargs.CommandModule<unknown, unknown> = {
  command: "list",
  describe: `
    List current DriveFile items that have an editing_session_key set
  `.trim(),
  builder: {},
  handler: async _argv => {
    await runWithPlatform("editing_session list", async ({ spinner: _spinner, platform }) => {
      console.error("\n");
      await report(platform);
      console.error("\n");
    });
  },
};
export default command;