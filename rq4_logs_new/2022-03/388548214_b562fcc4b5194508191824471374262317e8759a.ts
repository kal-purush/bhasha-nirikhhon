/**
 * Use a local copy of this repo for development
 * 1. Check out the repo https://github.com/metapages/cloudseed-deno at: `./cloudseed/git/cloudseed-deno`
 * 2. Change the env var `DENO****??` in `.env` files to point to the above repo, so that [just](https://github.com/casey/just) files will consume the updated reference, and point all deno scripts to the local git repo.
 * 3. Edit and update
 * 4. After: make a PR with the new changes
 */

import { join } from "https://deno.land/std@0.130.0/path/mod.ts";
import {
  ensureDirSync,
  existsSync,
} from "https://deno.land/std@0.130.0/fs/mod.ts";
import { exec, OutputMode } from "../exec/mod.ts";
import { getGitRepositoryRoot } from "../git/mod.ts";
import { removeLineInFile } from "../fs/mod.ts";

// Make sure the git repo is cloned to the local folder
const gitRoot = await getGitRepositoryRoot();
const cloudseedCloneRoot = join(gitRoot, ".cloudseed/git");
ensureDirSync(cloudseedCloneRoot);
const cloudseedGitDir = join(cloudseedCloneRoot, "cloudseed-deno");
if (!existsSync(cloudseedGitDir)) {
  const process = await exec(
    "git clone https://github.com/metapages/cloudseed-deno",
    { cwd: cloudseedCloneRoot, output: OutputMode.Tee }
  );
}

await removeLineInFile({
  file: join(gitRoot, ".env"),
  line: `DENO_SOURCE=${cloudseedGitDir}`,
});