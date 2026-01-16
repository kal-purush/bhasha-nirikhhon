import { defaultConfig as domtureDefaultConfig } from 'domture'

import { TestHarnessConfig } from './interfaces'

export const defaultConfig: TestHarnessConfig = { ...domtureDefaultConfig, loader: 'webpack', namespaces: {} }