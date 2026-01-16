import { AppComponent } from "./app.component";
import { loadActionData } from "./graph-structure";

export function generate(targetLanguage) {
    let requestUrl: string = AppComponent.explorerValues.endpointUrl;
    let requestMethod: string = AppComponent.explorerValues.selectedOption;
    let headers: any = AppComponent.explorerValues.headers;
    let postBody: string = AppComponent.explorerValues.postBody;
    let snippetString: string = "";
    switch(targetLanguage) {
        case "java":
            snippetString = generateJava(requestUrl, requestMethod, headers, postBody);
            break;
        case "c#":
            snippetString = generateCSharp(requestUrl, requestMethod, headers, postBody);
            break;
        case "javascript":
            snippetString = generateJavaScript(requestUrl, requestMethod, headers, postBody);
            break;
    }
    return snippetString;
};

function generateJava(requestUrl: string, requestMethod: string, headers: any, postBody: any): string {
    let urlComponents: string[] = requestUrl.split("?");
    let pathComponants: string[] = urlComponents[0].split("/");
    let snippetString: string = "IGraphServiceClient graphClient = GraphServiceClient\n.authenticationProvider(authenticationProvider)\n.buildClient();\nBaseEntity entity = graphClient";
    for (let i = 4; i < pathComponants.length; i++) {
        if (pathComponants[i]) {
            let pathString = pathComponants[i];
            let isId = pathString.match(".*\\d+.*");
            if (isId) {
                snippetString = snippetString.replace(/.$/, "\"" + pathString + "\")");
                continue;
            }

            snippetString = snippetString.concat("." + pathComponants[i].concat("()"));
        }
    }

    if (requestMethod === "POST" || requestMethod === "PATCH") {
        let lastPath = pathComponants[pathComponants.length - 1];
        let actionData = loadActionData("v1.0");
        if (actionData[lastPath] !== undefined) {
            console.log("action");
        } else {
            console.log("no action");
        }
        snippetString = snippetString.replace(/.$/, "{object representation of body})");
    }

    snippetString = snippetString.concat(".buildRequest()");
    if (requestMethod === "GET") {
        snippetString = snippetString.concat(".get();");
    } else if (requestMethod === "POST") {
        snippetString = snippetString.concat(".post();");
    } else if (requestMethod === "DELETE") {
        snippetString = snippetString.concat(".delete();");
    }
    return snippetString;
}

function generateCSharp(requestUrl: string, requestMethod: string, headers: any, postBody: any): string {
    let urlComponents: string[] = requestUrl.split("?");
    let pathComponants: string[] = urlComponents[0].split("/");
    let snippetString: string = "var graphClient = AuthenticationHelper.GetAuthenticatedClient();\n var baseObject = await graphClient";
    for (let i = 4; i < pathComponants.length; i++) {
        if (pathComponants[i]) {
            let pathString = pathComponants[i];
            let isId = pathString.match(".*\\d+.*");
            if (isId) {
                snippetString = snippetString.replace(/.$/, "(\"" + pathString + "\")");
                continue;
            }

            snippetString = snippetString.concat("." + pathString);
        }
    }

    snippetString = snippetString.concat(".Request()");
    if (requestMethod === "GET") {
        snippetString = snippetString.concat(".GetAsync();");
    }
    return snippetString;
}

function generateJavaScript(requestUrl: string, requestMethod: string, headers: any, postBody: string) {
    let urlComponents: string[] = requestUrl.split("?");
    let pathComponants: string[] = urlComponents[0].split("/");
    let snippetString: string = "";
    let clientInitString: string =
    `let client = MicrosoftGraph.Client.init({
        debugLogging: true,
        authProvider: function (done) {
            done(null, <AuthToken>);
        }
    });`;
    let postBodyObjectString: string = (postBody !== "") ? `let postBody = ${postBody}` : "";
    let url: string = "";
    for (let i = 4, l = pathComponants.length; i < l; i++) {
        url += `/${pathComponants[i]}`;
    }
    let callString: string = 
    `client
        .api("${url}")`;
    let headersString: string = "";
    if (typeof headers !== "undefined") {
        for(let i = 0; i < headers.length; i++) {
            let header = headers[i];
            if(header.name !== "") {
                headersString = `\n"${header.name}": "${header.value}"`;
            }
        }
    }
    if (headersString !== "") {
        callString +=
            `
            .headers({${headersString}
            })
            `;
    }       
        
    callString += 
        `.${(requestMethod === "GET") ? "get()" : ((postBodyObjectString !== "") ? "post(postBody)" : "post()")}
        .then((response) => {
            // Your response handler goes here
        })
        .catch((error) => {
            // Your error handler goes here
        });`;
    snippetString = 
    `${clientInitString}
${(requestMethod === "POST" && postBodyObjectString !== "") ? postBodyObjectString : ""}                   
${callString}`;
    return snippetString;
}