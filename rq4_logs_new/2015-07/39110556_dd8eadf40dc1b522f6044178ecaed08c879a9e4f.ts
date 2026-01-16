declare module "node-author-intrusion-split" {
    import types = require("node-author-intrusion");

    export interface NodeAuthorIntrusionSplitOptions {
        tokenizer?: string;
        stemmer?: string;
        normalization?: string[][];
    }
    export function process(args: types.AnalysisArguments): void;
}