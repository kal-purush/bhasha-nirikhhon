import { Eval } from "@ollyllm/ts-sdk/dist/eval";
import { ParseInteger } from "./ParseInteger";

export const ParseIntegerEval = new Eval({
    id: "ParseInteger",
    version: "0.1.0",
    prompt: ParseInteger,
    data: () => ([
        {
            input: "ONE",
            expected: 1,
        },
        {
            input: "two",
            expected: 2,
        },
        {
            input: "THREE.FIVE",
            expected: 3.5,
        },
    ]),
    run: async (input: string) => {
        const prompt = new ParseInteger();
        return await prompt.execute(input);
    },
    scoring: async (_input: string, expected: number, output: number) => {
        return output === expected ? 1 : 0;
    }
});