import * as semver from "https://deno.land/x/semver@v1.4.0/mod.ts";
import { exec, OutputMode } from "https://deno.land/x/exec@0.0.5/mod.ts";
import { getArgsFromEnvAndCli } from "../env/args_or_env.ts";

const opts = getArgsFromEnvAndCli({
  INCREMENT: "patch",
  REMOTE: "origin",
});

//let response = await exec('git ls-remote --tags origin', {output: OutputMode.Capture});
let response = await exec(`git ls-remote --tags ${opts.REMOTE}`, {
  output: OutputMode.Capture,
});
if (response.status.code !== 0) {
  console.log(response);
  throw new Error("Failed to list tags");
}
const tags = response.output
  .split("\n")
  .map((line) => line.trim())
  .map((line) => line.split("/")[line.split("/").length - 1])
  .filter((line) => line.length > 0)
  .filter((line) => semver.valid(line));
tags.sort(semver.compare);
const current = tags.length > 0 ? tags[tags.length - 1] : "0.0.0";
const next = semver.inc(current, opts.INCREMENT as semver.ReleaseType);
if (!next) {
  throw new Error(`Could not increment version from ${current}`);
}
response = await exec(`git tag v${next}`, { output: OutputMode.StdOut });
if (response.status.code !== 0) {
  console.log(response.output);
  throw new Error("Failed to create tag");
}
response = await exec(`git push origin v${next}`, {
  output: OutputMode.StdOut,
});
if (response.status.code !== 0) {
  console.log(response.output);
  throw new Error("Failed to push tag");
}
console.log(
  `Version v${semver.clean(current)} -> v${semver.clean(
    next
  )} (tag pushed to git origin)`
);