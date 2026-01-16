import { join } from "https://deno.land/std@0.130.0/path/mod.ts";
import {
  ensureFileSync,
  existsSync,
  copySync,
} from "https://deno.land/std@0.130.0/fs/mod.ts";
import { getArgsFromEnvAndCli } from "../../env/args_or_env.ts";
import { exec, OutputMode } from "../../exec/mod.ts";
import { isInsideDocker, isDockerImage, buildDockerImage } from "./mod.ts";
import { getGitRepositoryRoot } from "../../git/mod.ts";

export const hoistFileSystemIntoDockerContainer: (opts: {
  IMAGE: string;
  // CONTEXT?: string;
  USER?: string;
  COMMAND?: string;
  MOUNT_HISTORY?: boolean;
  MOUNT_ALIASES?: boolean;
  MOUNT_SSH?: boolean;
  MOUNT_GIT_CONFIG?: boolean;
}) => Promise<void> = async (opts) => {
  const {
    IMAGE,
    // CONTEXT = ".",
    USER = "root",
    MOUNT_HISTORY = true,
    MOUNT_ALIASES = true,
    MOUNT_SSH = true,
    MOUNT_GIT_CONFIG = true,
    COMMAND = "",
  } = opts;
  const ROOT = await getGitRepositoryRoot();
  const HOME = Deno.env.get("HOME");

  if (MOUNT_HISTORY) {
    ensureFileSync(join(ROOT, ".tmp/.bash_history"));
  }
  if (MOUNT_ALIASES) {
    ensureFileSync(join(ROOT, ".tmp/.aliases"));
    if (existsSync(`${HOME}/.aliases`)) {
      copySync(`${HOME}/.aliases`, join(ROOT, ".tmp/.aliases"));
    }
  }

  const WORKSPACE: string = Deno.env.get("WORKSPACE") || "/repo";

  const APP_PORT = Deno.env.get("APP_PORT");
  const APP_FQDN = Deno.env.get("APP_FQDN");
  const DOCKER_IMAGE_PREFIX = Deno.env.get("DOCKER_IMAGE_PREFIX");
  const DOCKER_TAG = Deno.env.get("DOCKER_TAG") || "latest";

  const userHome = USER === "root" ? "/root" : `/home/${USER}`;
  const maybeMountGitConfig =
    HOME && MOUNT_GIT_CONFIG && existsSync(join(HOME, ".gitconfig"))
      ? `-v ${join(HOME, ".gitconfig")}:${userHome}/.gitconfig`
      : "";
  const maybeMountSshConfig =
    HOME && MOUNT_SSH && existsSync(join(HOME, ".ssh"))
      ? `-v ${join(HOME, ".ssh")}:${userHome}/.ssh`
      : "";

  const containerWorkspacePath = Deno.cwd().replace(`${ROOT}`, WORKSPACE);

  const maybeBindPort = APP_PORT ? `-p ${APP_PORT}:${APP_PORT}` : "";

  const maybeAddHost = APP_FQDN ? `--add-host ${APP_FQDN}:127.0.0.1` : "";

  // If developing locally with deno, mount in the container
  const maybeMountLocalDeno =
    Deno.env.get("DENO_SOURCE") &&
    !Deno.env.get("DENO_SOURCE")!.startsWith("http")
      ? `-v ${Deno.env.get("DENO_SOURCE")}:${Deno.env.get("DENO_SOURCE")}`
      : "";
    const maybeSetDenoEnvVar =
      Deno.env.get("DENO_SOURCE")
        ? `-e DENO_SOURCE=${Deno.env.get("DENO_SOURCE")}`
        : "";

  console.log('Deno.env.get("DENO_SOURCE")', Deno.env.get("DENO_SOURCE"));

  const command = `docker run \
--rm \
-ti \
-e DOCKER_IMAGE_PREFIX=${DOCKER_IMAGE_PREFIX} \
-e DOCKER_IMAGE_PREFIX=${DOCKER_IMAGE_PREFIX} \
-e DOCKER_TAG=${DOCKER_TAG} \
-e HISTFILE=${WORKSPACE}/.tmp/.bash_history \
-e WORKSPACE=${WORKSPACE} \
-v ${ROOT}:${WORKSPACE} \
${maybeMountGitConfig} \
${maybeMountSshConfig} \
${maybeBindPort} \
${maybeAddHost} \
${maybeMountLocalDeno} \
${maybeSetDenoEnvVar} \
-w ${containerWorkspacePath} \
${IMAGE} ${COMMAND}`;

  await exec(command, {
    output: OutputMode.StdOut,
  });
};

if (isInsideDocker()) {
  Deno.exit(0);
}

const opts = getArgsFromEnvAndCli({
  IMAGE: true,
  CONTEXT: ".",
  DOCKERFILE: false,
  USER: "root",
  MOUNT_HISTORY: false,
  MOUNT_ALIASES: false,
  MOUNT_SSH: false,
  MOUNT_GIT_CONFIG: false,
  COMMAND: false,
});

const isImage = await isDockerImage(opts.IMAGE);
if (!isImage) {
  await buildDockerImage({
    tag: opts.IMAGE,
    context: opts.CONTEXT,
    dockerfile: opts.DOCKERFILE,
  });
}

await hoistFileSystemIntoDockerContainer({
  IMAGE: opts.IMAGE,
  USER: opts.USER,
  MOUNT_HISTORY: !!opts.MOUNT_HISTORY,
  MOUNT_ALIASES: !!opts.MOUNT_ALIASES,
  MOUNT_SSH: !!opts.MOUNT_SSH,
  MOUNT_GIT_CONFIG: !!opts.MOUNT_GIT_CONFIG,
  COMMAND: opts.COMMAND,
});