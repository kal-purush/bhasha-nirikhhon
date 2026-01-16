import OptionModel from "./Model";
import {
    modelGraphStyle,
    OptionPrice,
    ILimits
} from "./types";
import { ChartDataset } from 'chart.js'

export default class Utils {
    static ModelGraphData(
        model: OptionModel, 
        priceType: OptionPrice,
        limits: ILimits,
        style: modelGraphStyle
    ): ChartDataset<'line'> {
        const start = limits.start
        const end = limits.end

        const isOptionEmpty = Object.values(model.Option).length === 0
        const isLimitEmpty = !limits

        if (isOptionEmpty || isLimitEmpty) {
            return { data: [] }
        }

        const data: number[] = []

        for (let i = start; i < end; i++) {
            data.push(priceType === OptionPrice.call ?
                model.callPrice(i) :
                model.putPrice(i)
            )
        }

        return {
            data,
            label: `${model.MODEL_NAME} ${priceType}`,
            pointStyle: style.pointStyle,
            borderColor: style.lineColor
        }
    }
}