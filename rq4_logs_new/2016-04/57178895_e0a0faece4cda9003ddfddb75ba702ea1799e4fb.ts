// See "resolve" in "webpack.config.js"
declare namespace AddModule {
    function add(n1: number, n2: number): number;
}

declare module "add_module" {
    export = AddModule;
}
