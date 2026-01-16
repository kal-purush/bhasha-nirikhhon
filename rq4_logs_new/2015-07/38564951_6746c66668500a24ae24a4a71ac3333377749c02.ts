
/// <reference path='typings/node/node.d.ts'/>
/// <reference path='typings/express/express.d.ts'/>
/// <reference path='typings/react/react.d.ts'/>
/// <reference path='typings/glob/glob.d.ts'/>

import {readFileSync as readFile} from 'fs';
import {Application, Request, Response} from 'express';
import {} from 'path';
import * as glob from 'glob';

let app: Application;
let imports: string[] = [];
let importNames: string[] = [];
let importPaths: string[] = [];
let urlPaths: string [] = [];

export interface Pages {
    [page: string]: (page: Page) => void;
}

export interface Layout<P, S> extends React.Component<P, S> {
    name: string;
}

export interface Doc<P, S> extends React.Component<P, S> {
    docProps?: DocProps;
}

export interface Contents {
    [index: string]: React.Component<any, any>;
}

export interface PlatformDetect {
    name: string;
    
    /**
     * Specify a function to detect this platform.
     */
    detect(req: Request): boolean;
}

interface ComposerOptions {
    
    /**
     * Path to client configuration path.
     */
    clientConfPath: string;
    
    /**
     * If you have set the flag `inProduction` to true. You would need to provide 
     * a development to production client configuration path mapping for your 
     * configuration files.   
     */
    devToProdClientConfPath?: { [index: string]: string };
    
    /**
     * Set this flag to true if you are in production mode. Each page will use
     * production document properties instead of developement document properties.
     */
    inProduction?: boolean;
    
    /**
     * Add a namespace to your resource URL:s.
     */
    resourceNamespace?: string;
    
    /**
     * Specify the root path. All you URI:s defined in all your pages will reference
     * this root path.
     */
    rootPath: string;
    
    /**
     * Set express application.
     */
    app: Application;
}

let options: ComposerOptions;

export function init(options: ComposerOptions) {
    options = options;
}

export function pages<T, U>(pages: Pages): (req: T, res: U, next?: any) => void {
    return function(req: T, res: U) {
        
    }
}

/**
 * When all your pages is defined, call this function to generate a client composer.
 */
export function generateComposer(): void {
    
}

interface Platform {
    imports: string[];
    importNames: string[];
    doc?: Doc<any, any>;
    layout?: Layout<any, any>;
    contents?: Contents;
    detect(req: Request): boolean;
}

interface DocProps {
    title: string;
    confs: string[];
}

/**
 * The Page class builds up the whole HTML for your website.
 * You can specify document, layout, module and components to
 * customize the html you waant
 */
export class Page {
    
    /**
     * A flag for checking if this page have attached a URL handler.
     */
    private attachedUrlHandler: boolean = false;
    private platforms: { [index: string]: Platform } = {};
    private currentPlatform: Platform;
    private currentPlatformName: string;
    
    constructor(private url: string) {}
    
    /**
     * Specify a platform with a PlatformDetect.
     */
    public onPlatform(platform: PlatformDetect): Page {
        this.platforms[platform.name] = {
            imports: [],
            importNames: [],
            detect: platform.detect
        }
        
        this.currentPlatform = this.platforms[platform.name];
        
        return this;
    }
    
    /**
     * Define which document this page should have.
     */
    public hasDoc<P, S>(doc: Doc<P, S>): Page {
        if (typeof this.currentPlatform === 'undefined') {
            throw new TypeError('You must define a platform with `.onPlatform()` method.');
        }
        
        this.currentPlatform.doc = doc;
        
        return this;
    }
    
    /**
     * Define which layout this page should have.
     */
    public hasLayout<P, S>(layout: Layout<P, S>): Page {
        this.currentPlatform.layout = layout;
        
        return this;
    }
    
    /**
     * Define the document properties this page should have.
     */
    public withProps(props: DocProps): Page {
        props.confs = props.confs.map(conf => {
            let path = '';

            if(options.resourceNamespace) {
                path += `/${options.resourceNamespace}/`;
            }

            if(options.inProduction) {
                return path + glob.sync(`${options.clientConfPath}/*.${options.devToProdClientConfPath[conf]}.js`, { cwd: rootPath })[0];
            }

            return  `${path}/${options.clientConfPath}/${options.devToProdClientConfPath[conf]}.js`;
        });
        
        if (!props.title) {
            props.title = '';
        }
        
        this.currentPlatform.doc.props = props;
        
        return this;
    }
    
    public withContents(contents: Contents): Page {
        this.currentPlatform.contents = contents;
        this.serve();
        
        return this;
    }
    
    private serve(): void {
        this.addContents();
        this.addPages();
        if(!this.attachedUrlHandler) {
            app.get(this.url, this.next);
            this.attachedUrlHandler = true;
        }
    }
    
    private addContents(): void {
    }
    
    private addPages(): void {
        
    }
    
    private next(req: Request, res: Response, next: () => void): void {
        
    }
    
    private getContents(): void {
    }
}