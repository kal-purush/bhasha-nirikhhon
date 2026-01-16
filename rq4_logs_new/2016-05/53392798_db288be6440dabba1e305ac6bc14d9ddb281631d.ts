import { ContentResult } from "../View/ContentResult";
import { DownloadResult} from "../View/DownloadResult";
import { FileResult } from "../View/FileResult";
import { JsonResult } from "../View/JsonResult";
import { RedirectResult } from "../View/RedirectResult";
import { ViewResult } from "../View/ViewResult";
import { Controller } from "./Controller";
import {Request} from "../Http/Request";
import {Response} from "../Http/Response";

/**
 * TS-Framework HttpController
 * Base Controller to handle http requests
 */
export class HttpController extends Controller
{
    /**
     * The request made by the client
     * @type {Request}
     */
    protected request: Request;

    /**
     * Response sent by the controller action
     * @type {Response}
     */
    protected response: Response;

    /**
     * Method to trigger the actual responce sending
     */
    protected send: (IActionResult) => void;

    /**
     * Sets the request of the client requesting the resource
     * @param {Request} request
     */
    public __setRequest(request: Request): void
    {
        this.request = request;
    }

    /**
     * Sets the response the server will return to the client
     * @param {Response} response
     */
    public __setResponse(response: Response): void
    {
        this.response = response;
    }

    /**
     * Set the sending method to call
     * @param send
     * @private
     */
    public __setSend(send: (IActionResult) => void)
    {
        this.send = send;
    }

    /**
     * Clasic redirect type
     * @param {string} url
     * @param {number} status
    */
    protected redirect(url: string, status: number = 302) {
        this.send(new RedirectResult(url, status));
    }

    /**
     * Send string content
     * @param {string} text
     * @param {string?} contentType
    */
    protected content(text: string, contentType?: string) {
        this.send(new ContentResult(text, contentType));
    }

    /**
     * Send json content     
     * @param {} data
    */
    protected json(data: {}) {
        this.send(new JsonResult(data));
    }
    
    /**
     * Send file content
     * @param {string} path     
    */
    protected file(path: string) {
        this.send(new FileResult(path));
    }
    
    /**
     * Send download response
     * @param {string} path
     * @param {string?} filename
    */
    protected download(path: string, filename?: string) {
        this.send(new DownloadResult(path, filename));
    }

    /**
     * Send view
     * @param {string} template
     * @param {?} options
    */
    protected view(template: string, options?: {}) {
        var result = new ViewResult(template);
        if (options) result.options = options;
        this.send(result);
    }
}