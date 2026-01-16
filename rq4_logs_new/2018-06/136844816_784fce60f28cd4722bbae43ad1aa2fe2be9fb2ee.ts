import {CalculatorView} from '../views/calculator_view';
import {Observer} from '../core/observer';
import {EventEnum} from "../enums/event_enums";
import {CalculatorModel, IModelCalculationPartialDisplay, IModelCalculationResult} from '../model/calculator_model';
import {ButtonEnum} from "../enums/button_enums";
import {specialFunctionFromMath} from '../enums/special_functions';
import * as calculationHelper from '../helpers/calculation_helper';

export class CalculatorController extends Observer{
    private view: CalculatorView;
    private model: CalculatorModel;
    private expectedSecondArgument: boolean = false;
    private specialFunctionOpened: string[] = [];
    private subscriptOpen: boolean = false;

    constructor() {
        super();

        this.view = new CalculatorView();
        this.model = new CalculatorModel();
        this.attachEvents();
    }
    attachEvents(): void {
        this.view.on(this, EventEnum.CALCULATOR_BUTTON_CLICKED, this.onCalcButtonClick.bind(this));
        this.view.on(this, EventEnum.CALCULATOR_CANCEL_CLICKED, this.onCalcCancelButtonClick.bind(this));
        this.view.on(this, EventEnum.CALCULATOR_EQUAL_CLICKED, this.onEqualButtonClick.bind(this));
        this.view.on(this, EventEnum.CALCULATOR_SPECIAL_FUNCTION_CLICKED, this.onSpecialFunctionButtonClick.bind(this));

        this.model.on(this, EventEnum.CALCULATOR_MODEL_UPDATED, this.onCalcOperationsModelUpdated.bind(this));
        this.model.on(this, EventEnum.CALCULATOR_MODEL_SCORE_CALCULATED, this.onCalcModelScoreCalculated.bind(this));
        this.model.on(this, EventEnum.CALCULATOR_MODEL_RESET, this.onCalcModelReset.bind(this));
    }
    onCalcButtonClick(data: {value: string}): void {
        this.model.updateOperations(data.value);
    }
    onCalcCancelButtonClick(): void {
        this.view.resetLastOperation();
        this.model.reset();
    }
    onEqualButtonClick(): void {
        this.model.calculateScore();
    }
    onCalcModelReset(): void {
        this.view.resetDisplay();
    }
    onSpecialFunctionButtonClick(data: {value: string}): void {
        if (')' !== data.value) {
            this.specialFunctionOpened.push(data.value);
        }

        switch (data.value) {
            case '(':
                this.model.updateOperations(data.value);
                break;
            case ')':
                if (true === this.expectedSecondArgument) {
                    this.expectedSecondArgument = false;
                    if (this.subscriptOpen) {
                        this.model.updateOperations(')');
                        this.view.stopSubscript();
                        this.model.updateOperations('(');
                        this.subscriptOpen = false;
                    }
                } else if (this.specialFunctionOpened.length > 0) {
                    this.model.updateOperations(data.value);
                    this.performPartialCalculation(<string>this.specialFunctionOpened.pop());
                }
                break;
            case '!':
                this.model.updateOperations(`${data.value}`);
                this.performPartialCalculation(<string>this.specialFunctionOpened.pop());
                break;
            case 'log':
                this.expectedSecondArgument = true;
                this.model.updateOperations(`${data.value}`);
                this.view.startSubscript();
                this.subscriptOpen = true;
                this.model.updateOperations('(');
                break;
            default:
                this.model.updateOperations(`${data.value}(`);
        }
    }
    onCalcOperationsModelUpdated(data: IModelCalculationPartialDisplay): void {
        this.view.addToDisplay(data.value);
    }
    public performPartialCalculation(value: string): void {
        const modelPartialResult: string = this.model.getPartialResult();
        const lastFunctionOccurence: number = modelPartialResult.lastIndexOf(value);
        const lastClosingBracketOccurence: number = modelPartialResult.lastIndexOf(')');
        let partialResultToEval: string = modelPartialResult.substring(lastFunctionOccurence, lastClosingBracketOccurence + 1);
        let evaluatedPartialResult: number;
        let bracketsContent: string;

        if ('(' === value) {
            evaluatedPartialResult = eval(partialResultToEval);
        } else {
            bracketsContent = partialResultToEval.substring(
                partialResultToEval.indexOf('(') + 1,
                partialResultToEval.indexOf(')')
            );
            if ('^' === value) {
                const powerCalculationResult = calculationHelper.calculatePower(modelPartialResult);

                evaluatedPartialResult = powerCalculationResult.value;
                partialResultToEval = modelPartialResult.substring(
                    powerCalculationResult.index + 1,
                    lastClosingBracketOccurence + 1
                );
            } else if ('log' === value) {
                evaluatedPartialResult = calculationHelper.calculateLogarithm(modelPartialResult);
            } else if ('!' === value) {
                const factorialCalculationResult = calculationHelper.calculateFactorial(modelPartialResult);

                evaluatedPartialResult = factorialCalculationResult.value;
                partialResultToEval = modelPartialResult.substring(
                    factorialCalculationResult.index + 1,
                    lastFunctionOccurence + 1
                );
            } else {
                evaluatedPartialResult = specialFunctionFromMath[value](eval(bracketsContent));
            }
        }

        if (isNaN(evaluatedPartialResult)) {
            this.model.forceSetResult(NaN);
        } else {
            this.model.setPartialResult(modelPartialResult.replace(
                partialResultToEval,
                evaluatedPartialResult.toString()
            ));
        }
    }
    onCalcModelScoreCalculated(data: IModelCalculationResult): void {
        const {
            result
        } = data;

        if (isNaN(result)) {
            this.model.reset();
            this.view.updateDisplay('Syntax error');
        } else {
            this.view.updateLastOperation();
            this.view.updateDisplay(result.toString());
        }
    }
}