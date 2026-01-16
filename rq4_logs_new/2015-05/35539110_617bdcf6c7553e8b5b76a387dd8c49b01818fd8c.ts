import mod = require('../Module');
import constants = require("../../../App/Constants");
import ${CLASS_NAME}Service = require("../Services/${CLASS_NAME}Service")

interface I${CLASS_NAME}Scope extends ng.IScope {
    ${CLASS_NAME}Contracts: ${CLASS_NAME}Service.I${CLASS_NAME}Contracts
}

class ${CLASS_NAME}Controller {
    constructor(
        \$scope: I${CLASS_NAME}Scope,
        ${DS}${CLASS_NAME}Service: ${CLASS_NAME}Service.I${CLASS_NAME}Service
        ) {

        \$scope.${CLASS_NAME}Contracts = ${DS}${CLASS_NAME}Service.Contracts;

    }
}
mod.${MODULE_NAME}Module.RegisterRouteController(
    "",
    "Templates/${CLASS_NAME}.html",
    ${CLASS_NAME}Controller,
    constants.\$typedScope<I${CLASS_NAME}Scope>(),
    ${CLASS_NAME}Service.${DS}${CLASS_NAME}Service
);