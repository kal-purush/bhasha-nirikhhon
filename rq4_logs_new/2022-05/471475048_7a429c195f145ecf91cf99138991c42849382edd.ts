import { IOptionModel, IComparingOptionGraphData, OptionPrice } from "@/assets/types";

export default abstract class OptionCalculator {
    public static graphDataForComparing(
        models: IOptionModel[],
        priceTypes: OptionPrice[],
        graphLength: number
    ): IComparingOptionGraphData[] {
        const result: IComparingOptionGraphData[] = []

        for (const model of models.values()) {
            for (const priceType of priceTypes.values()) {
                const modelName = model.name
                const data: number[] = this.callPrices(model, priceType, graphLength)

                result.push({
                    modelName,
                    data,
                    priceType
                })
            }
        }

        return result
    }
    private static callPrices(
        model: IOptionModel,
        priceType: OptionPrice,
        graphLength: number
    ) {
        const data: number[] = []

        const dataFunction = priceType === OptionPrice.call ?
            model.callPrice :
            model.putPrice

        for (let index = 0; index < graphLength; index++) {
            data.push(dataFunction.bind(model)(graphLength))
        }

        return data
    }
}