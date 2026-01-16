
/// <reference path='../../../../typings/express/express.d.ts' />


import * as path from 'path';
import { defaultConfigs, Document } from './components/Document';
import { Layout } from './components/Layout';
import { NavigationBar, TodoList, TodoItem } from './components/Contents';
import express = require('express');
import { Page, Pages } from '../../../../src/composer/serverComposer';

const WebPlatform = { name: 'all', detect: (req: express.Request) => true };

export = function(componentFolderPath: string): Pages {
    const DocumentComponent = { component: Document, importPath: path.join(componentFolderPath, 'Document.js') };
    const LayoutComponent = { component: Layout, importPath: path.join(componentFolderPath, 'Layout.js') };
    const contentPath = path.join(componentFolderPath, 'Contents.js');

    return {
        '/': (page: Page) => {
            page.onPlatform(WebPlatform)
                .hasDocument(DocumentComponent, defaultConfigs)
                .hasLayout(LayoutComponent, {
                    NavigationBar: { component: NavigationBar, importPath: contentPath },
                    TodoList: { component: TodoList, importPath: contentPath },
                })
                .end();
        },
        '/todo': (page: Page) => {
            page.onPlatform(WebPlatform)
                .hasDocument(DocumentComponent, defaultConfigs)
                .hasLayout(LayoutComponent, {
                    TodoItem: { component: TodoItem, importPath: contentPath },
                })
                .end();
        }
    }
}