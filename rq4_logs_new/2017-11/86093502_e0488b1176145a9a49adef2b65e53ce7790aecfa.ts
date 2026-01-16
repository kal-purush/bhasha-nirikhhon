export namespace DocumentServiceOptions {

    export interface Config {
        apiKey: string;
        apiSecret: string;
        apiCustomer: string;
        host?: string;
        logging?: boolean;
    }

    export interface RequestOptions {
        path?: string;
        uri?: string;
        postData?: any;
        resolveWithFullResponse?: boolean;
        json?: boolean;
        statusCodes?: { [key: number]: string };
        headers?: { [key: string]: any };
        body?: any;
        forrmData?: any;
        method?: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'COPY' | 'HEAD';
    }

    export interface FileConfig {
        directory: string;
        filename: string;
        fileExtension?: string;
    }

    export interface SigningData {
        // Everything in this top section is returned by the Document Service Sign Upload
        key: string;
        AWSAccessKeyId: string;
        acl: string;
        policy: string;
        signature: string;
        url: string;
        // The optional properties in this bottom section are configured in this client and returned to calling functions
        originalFilename?: string;
        fileExtension?: string;
    }

    export interface DownloadInfo {
        url: string;
        expiration: number;
    }

    export interface SigningDataByFile {
        [index: string]: SigningData;
    }
}