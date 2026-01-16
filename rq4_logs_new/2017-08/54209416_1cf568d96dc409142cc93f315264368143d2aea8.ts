import { Command } from "./Command";
import { CommandResult } from "./CommandResult";
import { LineResults } from "./LineResults";
import { Parameter } from "./Parameters/Parameter";
import { RepeatingParameters } from "./Parameters/RepeatingParameters";
import { SingleParameter } from "./Parameters/SingleParameter";
import { CaseStyle } from "../Languages/Casing/CaseStyle";

/**
 * A command for the beginning of a static function.
 */
export class StaticFunctionCommand extends Command {
    /**
     * Information on parameters this command takes in.
     */
    private static parameters: Parameter[] = [
        new SingleParameter("privacy", "The privacy of the function.", true),
        new SingleParameter("className", "The name of the class the function is on.", true),
        new RepeatingParameters(
            "Function parameters.",
            [
                new SingleParameter("parameterName", "A named parameter for the function.", true),
            ])
    ];

    /**
     * @returns Information on parameters this command takes in.
     */
    public getParameters(): Parameter[] {
        return StaticFunctionCommand.parameters;
    }

    /**
     * Renders the command for a language with the given parameters.
     * 
     * @param parameters   The command's name, followed by any parameters.
     */
    public render(parameters: string[]): LineResults {
        let publicity: string = parameters[1];
        let className: string = parameters[2];
        let functionName: string = parameters[3];

        let output: string = className + ".";
        output += this.getPublicityPrefix(publicity);
        output += this.context.convertStringToCase(functionName, this.getPublicityCase(publicity));
        output += "(";

        if (parameters.length > 4) {
            output += parameters[4];

            for (let i: number = 5; i < parameters.length; i += 1) {
                output += ", ";
                output += parameters[i];
            }
        }

        output += ")";

        return LineResults.newSingleLine(output, true);
    }

    /**
     * Determines the case style for a static function.
     * 
     * @param publicity   Publicity of the static function.
     * @returns Case style for the publicity.
     */
    private getPublicityCase(publicity: string): CaseStyle {
        if (publicity === "private") {
            return this.language.properties.classes.statics.functions.privateCase;
        }

        if (publicity === "protected") {
            return this.language.properties.classes.statics.functions.protectedCase;
        }

        return this.language.properties.classes.statics.functions.publicCase;
    }

    /**
     * Determines the name prefix for a static function.
     * 
     * @param publicity   Publicity of the static function.
     * @returns Name prefix for the publicity.
     */
    private getPublicityPrefix(publicity: string): string {
        if (publicity === "private") {
            return this.language.properties.classes.statics.functions.privatePrefix;
        }

        if (publicity === "protected") {
            return this.language.properties.classes.statics.functions.protectedPrefix;
        }

        return this.language.properties.classes.statics.functions.publicPrefix;
    }

    /**
     * Generates a string for a parameter.
     * 
     * @param parameters   An ordered sequence of [parameterName, parameterType, ...].
     * @param i   An index in the parameters of a parameterName.
     * @remarks This assumes that if a language doesn't declare variables, it doesn't declare types.
     */
    private generateParameterVariable(parameters: string[], i: number): string {
        if (!this.language.properties.variables.declarationRequired) {
            return parameters[i];
        }

        let parameterName: string = parameters[i];
        let parameterType: string = this.context.convertCommon("type", parameters[i + 1]);

        return this.context.convertParsed(["variable inline", parameterName, parameterType]).commandResults[0].text;
    }
}