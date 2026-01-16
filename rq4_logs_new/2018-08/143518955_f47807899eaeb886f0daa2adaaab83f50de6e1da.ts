import * as yml from "js-yaml";
import * as fs from "fs"
import * as req from "request";

import { getLogger } from 'log4js';
import { Request } from './objects/request'
import { Ressource } from './objects/ressource';
import { Response } from './objects/response';
import { ParameterInfo } from './objects/parameterinfo';
import { UseReference } from './objects/useReference';
import { ExecutionStatisticResult } from './objects/executionstatistic';
import {SwaggerRunner} from "./openAPI/SwaggerRunner";

interface SwaggerDefinition {
    host: string,
    basePath: string,
    schemes: string[],
    paths: string[]
}

interface Options {
    method: string,
    url: string,
    body: any,
    json: any,
    auth: any,
    headers: any
}

export class SwaggerAPITester {
    ressources: Ressource[];
    cachedValues: Map<string, object>;
    logger: any;
    executedRequests: string[];
    executedRessources: string[];
    ExecutionResults: Map<string, Response>;
    user: string;
    password: string;

    private swaggerRunner: SwaggerRunner;

    constructor(p_swaggerfilepath: string) {
        this.executedRequests = [];
        this.executedRessources = [];
        this.ressources = [];
        this.cachedValues = new Map<string, object>();
        this.ExecutionResults = new Map<string, Response>();
        this.logger = getLogger();
        this.logger.level = 'debug';

        this.loadRessourcesFromSwaggerFile(p_swaggerfilepath);
        //todo: configuration der Logger abschlie�en -> Video Server Projekt -> mit Andy vor Ort
    }

    private loadRessourcesFromSwaggerFile(p_file: string) {
        let doc: SwaggerDefinition = {
            host: '',
            basePath: '',
            schemes: [],
            paths: []
        };

        try {
            this.logger.info("Load swagger file: " + p_file);
            doc = yml.safeLoad(fs.readFileSync(p_file, 'utf8'));
        } catch (e) {
            console.log(e);
            return;
        }
        let host = doc.host;

        if (doc.basePath) {
            host = host + doc.basePath
        }
        const scheme = doc.schemes[0];

        const paths = Object.keys(doc.paths);

        for (let i = 0; i < paths.length; i++) {
            const path: any = paths[i];
            const pathcontent = doc.paths[path];
            const url = scheme + "://" + host + path;
            const pathrequests = this.resolveRessourceRequests(pathcontent, url);

            //sort by pathrequests
            pathrequests.sort(function (obj1) {
                if (obj1.method == 'post')
                    return -1;
                if (obj1.method == 'put')
                    return 0;
                else if (obj1.method == 'patch')
                    return 1;
                else if (obj1.method == 'get')
                    return 2;
                else
                    return 3;
            });
            this.ressources.push(new Ressource(path, pathrequests));
        }
    };

    private resolveRessourceRequests(p_pathcontent: any, p_url: string) {
        let methods = Object.keys(p_pathcontent);
        let pathrequests = [];
        for (let j = 0; j < methods.length; j++) {
            let method: any = methods[j];
            let content = p_pathcontent[method];
            let examples = content['x-examples'];
            let parameters = content.parameters;

            if (!parameters) {
                parameters = [];
            }
            if (!examples) {
                examples = [];
            }

            if (examples.length == 0) {
                this.logger.warn("No exammple found for path url: '" + p_url + "' : method:  " + method);
                pathrequests.push(new Request("unknown", method, p_url, false));
            }
            else {
                for (let e = 0; e < examples.length; e++) {
                    let example = examples[e];

                    let request = new Request(example.name, methods[j], p_url, true);
                    request.expectedresponse = example.expectedresponse;

                    //references
                    if (example.use != undefined) {
                        for (let u = 0; u < example.use.length; u++) {
                            let refcontent = example.use[u];
                            let useRef = new UseReference();

                            if (refcontent.ressource) {
                                useRef.ressourcepath = refcontent.ressource;
                            }
                            if (refcontent.id) {
                                useRef.uid = refcontent.id;
                            }
                            if (refcontent.example) {
                                useRef.examplename = refcontent.example;
                            }
                            request.useReference.push(useRef);
                        }
                    }

                    for (let k = 0; k < parameters.length; k++) {
                        const parameter = parameters[k];
                        let parameterValue = example.parameters[parameter.name];

                        if (!parameterValue) {
                            //todo: check and set request invalid state only for required parameters
                            this.logger.warn("Example for Parameter not set: " + parameter.name);
                            //request.valid = false;
                        } else {
                            let resulttype = undefined;
                            if (parameterValue['attribute']) {
                                //find the right use reference
                                let newParameterValue: string;
                                newParameterValue = '$cache.';

                                let useId = parameterValue['use'];
                               
                                if (parameterValue['type'])
                                    resulttype = parameterValue['type'];

                                for (let refIndex = 0; refIndex < request.useReference.length; refIndex++) {
                                    let ref: UseReference = request.useReference[refIndex];

                                    if (ref.uid == useId) {
                                        newParameterValue = newParameterValue + ref.examplename;
                                    }
                                }
                                parameterValue = newParameterValue + "." + parameterValue['attribute'];
                                this.logger.debug("Attrubute Path: " + parameterValue);

                            } else if (parameterValue['value']) {
                                parameterValue = parameterValue['value'];
                            }
                            let parameterdescription = new ParameterInfo(parameter.name, parameter.in, parameterValue);

                            if (resulttype)
                                parameterdescription.format = resulttype;

                            request = this.setRequestValues(parameterdescription, request, true);
                        }
                    }


                    pathrequests.push(request);
                }
            }
        }
        return pathrequests;
    }

    private setRequestValues(parameterinfo: ParameterInfo, request: Request, initialResolution: boolean) {


        if (parameterinfo.value.indexOf("$cache") != -1) {
            if (initialResolution) {
                //cache is not ready yet, resolve values later
                request.resolveParametersLater = true;
                request.parametersToResolve.push(parameterinfo);
                return request;
            } else {

                parameterinfo.value = parameterinfo.value.replace("$cache.", "");

                if (parameterinfo.value.indexOf(",") != -1) {
                    this.logger.warn("More then one referenced Parameter is not implemented yet");
                } else {
                    let extractedPath = parameterinfo.value;
                    let cachePath: string[] = extractedPath.split('.');

                    let obj: any = this.cachedValues.get(cachePath[0]);
                    //try catch
                    try {
                        for (let p = 1; p < cachePath.length; p++) {
                            obj = obj[cachePath[p]];
                        }
                    } catch (exception) {
                        this.logger.error("Exception while resolving of cache value: ", exception);
                    }

                    this.logger.debug("Value loaded from cache: " + obj);
                    if (!parameterinfo.format)
                        parameterinfo.value = parameterinfo.value.replace(extractedPath, obj);
                    else if (parameterinfo.format == "array") {
                        parameterinfo.value = parameterinfo.value.replace(extractedPath, "["+ obj +"]");
                    }

                }

            }
        }

        if (parameterinfo.position == "path") {
            request.url = request.url.replace("{" + parameterinfo.name + "}", parameterinfo.value)
        }
        else if (parameterinfo.position == "body") {
            let bodyvalue = parameterinfo.value;

            if (bodyvalue.startsWith('{')) {
                let jsonValidString = JSON.stringify(eval("(" + bodyvalue + ")"));
                request.bodyparameter = JSON.parse(jsonValidString);
            }
            else if (bodyvalue.startsWith('[')) {
                let arraycontent = [];
                arraycontent.push(bodyvalue.replace('[', '').replace(']', ''));
                request.bodyparameter = arraycontent;
            }
            else {
                request.bodyparameter = bodyvalue;
            }


        }
        else if (parameterinfo.position == "query") {
            if (request.url.includes("?")) {
                request.url = request.url + "&" + parameterinfo.name + "=" + parameterinfo.value;
            }
            else {
                request.url = request.url + "/?" + parameterinfo.name + "=" + parameterinfo.value;
            }
        }
        else if (parameterinfo.position == "header") {
            request.headerParameter[parameterinfo.name] = parameterinfo.value;
        }

        return request;
    }

    public async Execute() {
        await this.executeAllRessources();
    }

    public executeAllRessources(): Promise<any> {
        return new Promise<any>(async (fulfill, reject) => {
            try {
                for (let i = 0; i < this.ressources.length; i++) {
                    const ressource = this.ressources[i];
                    console.log("------------- " + i + " -----------------");
                    await this.executeRessource(ressource);
                    console.log("------------- " + i + " -----------------");
                }
                fulfill(this.executedRequests);
            } catch(e) {
                reject(e);
            }

        });
    }

    private checkAndExecuteUseReferences(useRefs: UseReference[]): Promise<void> {
        return new Promise<any>(async (fulfill, reject) => {

            for (let i = 0; i < useRefs.length; i++) {
                let useRef = useRefs[i];
                let ressource = this.findRessourceByPath(useRef.ressourcepath);
                if (!req) {
                    reject("Ressource not found!");
                    return;
                }
                this.executeRessource(ressource).then(() => {
                    fulfill();
                }).catch((err) => {
                    this.logger.error(err);
                });
            }
        });
    }

    private executeRessource(ressource: Ressource): Promise<void> {
        return new Promise<any>(async (fulfill, reject) => {
            this.logger.info("Execute requests of ressource: " + ressource.path);
            //check ressource is already executed 
            if (this.IsRessourceListedAsExecuted(ressource.path)) {
                this.logger.debug("Ressource already executed! " + ressource.path);
                fulfill();
                return;
            }

            for (let j = 0; j < ressource.requests.length; j++) {
                const request = ressource.requests[j];

                try {
                    await this.executeRequest(ressource.path, request).catch((err) => {
                        this.logger.error(err);
                    });
                } catch (error) {
                    reject(error);
                    this.logger.error(error);
                    return;
                }
            }
            if (!this.IsRessourceListedAsExecuted(ressource.path)) {
                this.markRessourceAsExecuted(ressource.path);
            }
            fulfill();
        });
    }

    public async executeRequest(ressourcepath: string, myRequest: Request): Promise<void> {
        return new Promise<void>(async (fulfill, reject) => {
            this.logger.debug("Execute  " + req.name);
            //check request is already executed 
            if (this.IsRequestListedAsExecuted(ressourcepath, myRequest.name, myRequest.method)) {
                this.logger.debug("Request already executed! " + req.name);
                fulfill();
                return;
            }

            if (myRequest.valid) {
                //check reference and trigger the execution
                if (myRequest.useReference.length > 0) {
                    this.logger.debug("Execute Use References");
                    await this.checkAndExecuteUseReferences(myRequest.useReference);
                    this.logger.debug("Execute Use References finalized");
                }
                //replace cached parameters here!
                if (myRequest.resolveParametersLater) {
                    for (let p = 0; p < myRequest.parametersToResolve.length; p++) {
                        let param = myRequest.parametersToResolve[p];
                        myRequest = this.setRequestValues(param, myRequest, false);
                    }
                }

                // let responseObj = await
                this.sendRequest(myRequest).then((responseObj) => {
                    if (myRequest.method == "put" || myRequest.method == "post") {
                        this.cachedValues.set(myRequest.name,responseObj.body);
                    }

                    responseObj.result = responseObj.httpCode == myRequest.expectedresponse;
                    responseObj.resultmessage = "Expected result: " + myRequest.expectedresponse + ", actual result: " + responseObj.httpCode;

                    this.logger.debug(responseObj.resultmessage);
                    if (!responseObj.result)
                        this.logger.error("Result: " + responseObj.result);
                    else
                        this.logger.debug("Result: " + responseObj.result);
                    this.markRequestAsExecuted(ressourcepath, myRequest.name, myRequest.method, responseObj);
                    fulfill();


                }).catch((err) => {
                    this.logger.error(err);
                    reject(err);
                    return;
                });
            }
            else {
                reject("- can not execute '" + myRequest.method + "' request, request is invalid: " + myRequest.url);
            }
        });
    }

    private sendRequest(myRequest: Request): Promise<Response> {
        return new Promise<Response>((fulfill, reject) => {
            this.logger.debug("Send request to url: " + myRequest.url);

            const options: Options = {
                method: myRequest.method,
                url: myRequest.url,
                body: undefined,
                json: undefined,
                auth: undefined,
                headers: undefined
            };

            if (this.user && this.password) {
                options.auth = {
                    user: this.user,
                    pass: this.password,
                    sendImmediately: true
                }
            }

            if (myRequest.headerParameter) {
                options.headers = myRequest.headerParameter;
            }

            if (myRequest.bodyparameter) {
                options.body = myRequest.bodyparameter;
                options.json = true;
            }

            req(options, function (err, res, body) {
                if (err) {
                    reject(err);
                    return;
                }
                try {
                    let responseObj = new Response(res.headers, res.statusCode, body);
                    fulfill(responseObj);
                } catch (exception) {
                    reject("Sending request failed: " + exception);
                    return;
                }

            });
        });
    }

    // public getAllRessources() {
    //     return this.ressources;
    // }

    public findRessourceByPath(path: string): Ressource {
        for (let i = 0; i < this.ressources.length; i++) {
            let ressource = this.ressources[i];
            if (ressource.path == path) {
                return ressource;
            }
        }
        return undefined;
    }

    // public findRequestsByPathAndMethod(p_path: string, p_method: string): Request[] {
    //     let ressource = this.findRessourceByPath(p_path);
    //     let requests: Request[] = [];
    //     if (ressource == undefined) {
    //         return requests;
    //     }
    //     for (let i = 0; i < ressource.requests.length; i++) {
    //         let request = ressource.requests[i];
    //
    //         if (request.method == p_method) {
    //             requests.push(request);
    //         }
    //     }
    //     return requests;
    // }

    // public findRequestByPathAndExampleName(p_path: string, p_method: string, p_examplename: string): Request {
    //     let ressource = this.findRessourceByPath(p_path);
    //     let request: Request;
    //     if (ressource == undefined) {
    //         return undefined;
    //     }
    //     for (let i = 0; i < ressource.requests.length; i++) {
    //         let request = ressource.requests[i];
    //         if (request.name == p_examplename) {
    //             return request;
    //         }
    //     }
    //     return undefined;
    // }

    markRequestAsExecuted(p_ressourcepath: string, p_request_name: string, p_request_method: string, responseobject: Response) {
        const entry = `${p_ressourcepath}:${p_request_name}:${p_request_method}`;
        this.logger.debug("Mark Request as Executed: " + entry);
        this.executedRequests.push(entry);
        this.ExecutionResults.set(entry, responseobject);
    }

    private IsRequestListedAsExecuted(p_ressourcepath: string, p_request_name: string, p_request_method: string): boolean {
        const entry = `${p_ressourcepath}:${p_request_name}:${p_request_method}`;
        return this.executedRequests.indexOf(entry) != -1;
    }

    private markRessourceAsExecuted(p_ressourcepath: string): void {
        this.logger.debug("markRessourceAsExecuted: " + p_ressourcepath);
        this.executedRessources.push(p_ressourcepath)
    }

    private IsRessourceListedAsExecuted(p_ressourcepath: string): boolean {
        return this.executedRessources.indexOf(p_ressourcepath) != -1;
    }

    public setCommunicationAuth(p_user: string, p_password: string) {
        this.user = p_user;
        this.password = p_password;
    }

    public getTotalResult(): ExecutionStatisticResult {
        const result = new ExecutionStatisticResult();

        this.ExecutionResults.forEach((responseResult: Response) => {
            if (responseResult.result == true)
                result.totalPassed++;
            else if (responseResult.result == false)
                result.totalFailed++;
            else
                result.invalid++;
        });
        return result;
    }

    printTotalResult() {
        const result = this.getTotalResult();
        console.log("--------------------------------------------");
        this.logger.info("Total passed: " + result.totalPassed);
        this.logger.error("Total failed: " + result.totalFailed);
        this.logger.warn("Invalid Executions: " + result.invalid);
        console.log("--------------------------------------------");
    }
}