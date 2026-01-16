
/// <reference path='../../typings/nightmare/nightmare.d.ts'/>

import HeadlessWebBrowser = require('nightmare');
import { Pages, PlatformDetect } from '../../src/composer/serverComposer';

export interface BrowserDirectives {
    componentFolderPath: string;
    initialRoute: string;
    useBrowserActions?: (browser: HeadlessWebBrowser) => HeadlessWebBrowser;
    pages: Pages;
    useDefaultDocument: () => DocumentDeclaration;
    useDefaultLayout: () => LayoutDeclaration;
    useDefaultContent: (content: string) => ContentDeclaration;
    defaultConfigs: DocumentProps;
    defaultPlatform: PlatformDetect;
}