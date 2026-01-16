import { parseVersion } from '@ag-website-shared/utils/parseVersion';
import { agChartsVersion } from '@constants';
import type { ConfigFunction } from '@markdoc/markdoc';

export const chartsVersion: ConfigFunction = {
    transform() {
        const { major, minor } = parseVersion(agChartsVersion);
        return `${major}.${minor}`;
    },
};

export const chartsVersionPatch: ConfigFunction = {
    transform() {
        const { major, minor, patchNum } = parseVersion(agChartsVersion);
        return `${major}.${minor}.${patchNum}`;
    },
};