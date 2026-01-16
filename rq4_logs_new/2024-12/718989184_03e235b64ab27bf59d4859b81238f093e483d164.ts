import { join, relative, resolve } from 'node:path';

import {
  getConfigInfo,
  getProjectRoot,
  loadAllPresets,
  loadMainConfig,
  resolveAddonName,
  resolvePathInStorybookCache,
  validateFrameworkName,
  versions,
} from '@storybook/core/common';
import { oneWayHash } from '@storybook/core/telemetry';
import type { BuilderOptions, CLIOptions, LoadOptions, Options } from '@storybook/core/types';
import { global } from '@storybook/global';

import { MissingBuilderError } from '@storybook/core/server-errors';

import prompts from 'prompts';
import invariant from 'tiny-invariant';

import { getManagerBuilder, getPreviewBuilder } from './utils/get-builders';
import { getServerChannelUrl, getServerPort } from './utils/server-address';
import { updateCheck } from './utils/update-check';

export async function loadStorybook(
  options: CLIOptions &
    LoadOptions &
    BuilderOptions & {
      storybookVersion?: string;
      previewConfigPath?: string;
    }
): Promise<Options> {
  const { packageJson, versionUpdates } = options;
  let { storybookVersion, previewConfigPath } = options;
  const configDir = resolve(options.configDir);
  if (packageJson) {
    invariant(
      packageJson.version !== undefined,
      `Expected package.json#version to be defined in the "${packageJson.name}" package}`
    );
    storybookVersion = packageJson.version;
    previewConfigPath = getConfigInfo(packageJson, configDir).previewConfig ?? undefined;
  } else {
    if (!storybookVersion) {
      storybookVersion = versions.storybook;
    }
  }
  // updateInfo are cached, so this is typically pretty fast
  const [port, versionCheck] = await Promise.all([
    getServerPort(options.port, { exactPort: options.exactPort }),
    versionUpdates
      ? updateCheck(storybookVersion)
      : Promise.resolve({ success: false, cached: false, data: {}, time: Date.now() }),
  ]);

  if (!options.ci && !options.smokeTest && options.port != null && port !== options.port) {
    const { shouldChangePort } = await prompts({
      type: 'confirm',
      initial: true,
      name: 'shouldChangePort',
      message: `Port ${options.port} is not available. Would you like to run Storybook on port ${port} instead?`,
    });
    if (!shouldChangePort) {
      process.exit(1);
    }
  }

  const rootDir = getProjectRoot();
  const cacheKey = oneWayHash(relative(rootDir, configDir));

  const cacheOutputDir = resolvePathInStorybookCache('public', cacheKey);
  let outputDir = resolve(options.outputDir || cacheOutputDir);
  if (options.smokeTest) {
    outputDir = cacheOutputDir;
  }

  options.port = port;
  options.versionCheck = versionCheck;
  options.configType = 'DEVELOPMENT';
  options.configDir = configDir;
  options.cacheKey = cacheKey;
  options.outputDir = outputDir;
  options.serverChannelUrl = getServerChannelUrl(port, options);

  const config = await loadMainConfig(options);
  const { framework } = config;
  const corePresets = [];

  let frameworkName = typeof framework === 'string' ? framework : framework?.name;
  if (!options.ignorePreview) {
    validateFrameworkName(frameworkName);
  }
  if (frameworkName) {
    corePresets.push(join(frameworkName, 'preset'));
  }

  frameworkName = frameworkName || 'custom';

  // Load first pass: We need to determine the builder
  // We need to do this because builders might introduce 'overridePresets' which we need to take into account
  // We hope to remove this in SB8
  let presets = await loadAllPresets({
    corePresets,
    overridePresets: [
      require.resolve('@storybook/core/core-server/presets/common-override-preset'),
    ],
    ...options,
    isCritical: true,
  });

  const { renderer, builder, disableTelemetry } = await presets.apply('core', {});

  if (!builder) {
    throw new MissingBuilderError();
  }

  const builderName = typeof builder === 'string' ? builder : builder.name;
  const [previewBuilder, managerBuilder] = await Promise.all([
    getPreviewBuilder(builderName, options.configDir),
    getManagerBuilder(),
  ]);

  const resolvedRenderer = renderer && resolveAddonName(options.configDir, renderer, options);

  // Load second pass: all presets are applied in order
  presets = await loadAllPresets({
    corePresets: [
      require.resolve('@storybook/core/core-server/presets/common-preset'),
      ...(managerBuilder.corePresets || []),
      ...(previewBuilder.corePresets || []),
      ...(resolvedRenderer ? [resolvedRenderer] : []),
      ...corePresets,
    ],
    overridePresets: [
      ...(previewBuilder.overridePresets || []),
      require.resolve('@storybook/core/core-server/presets/common-override-preset'),
    ],
    ...options,
  });

  const features = await presets.apply('features');
  global.FEATURES = features;

  const fullOptions: Options = {
    ...options,
    presets,
    features,
  };

  return fullOptions;
}