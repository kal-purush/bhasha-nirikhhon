/// <reference path="Scripts/typings/knockout/knockout.d.ts" />
declare module Contract {
    interface IExercise {
    }
    interface IExerciseGenerator {
        name: string;
        generate: () => IExercise;
        getPrinter: (options: PrinterOptions) => IPrinter;
    }
    interface IPrinter {
        options: PrinterOptions;
        print: (exercises: IExercise[]) => void;
    }
    interface ISubjectViewModel {
        name: string;
        isSelected: KnockoutObservable<boolean>;
        getExerciseGenerator: () => IExerciseGenerator;
    }
    interface IExerciseGeneratorViewModel {
        name: string;
        isSelected: KnockoutObservable<boolean>;
        getExerciseGenerator: () => IExerciseGenerator;
    }
    interface PrinterOptions {
        rootElement: HTMLElement;
        includeResult: boolean;
    }
}