// Copyright (C) 2018-2019 Andreas Huber Dönni
//
// This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
// License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later
// version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with this program. If not, see
// <http://www.gnu.org/licenses/>.

import { Component, Prop } from "vue-property-decorator";
import { Asset } from "../model/Asset";
import { ComponentBase } from "./ComponentBase";
import { Format } from "./Format";


export const numericColumnNames = ["fineness", "unitValue", "quantity", "totalValue", "percent"] as const;
export type NumericColumnName = keyof Pick<Asset, typeof numericColumnNames[number]>;

@Component
/** Implements a text field control that simplifies common functionality like validation. */
// eslint-disable-next-line import/no-default-export
export default class NumericTableCell extends ComponentBase<number> {
    @Prop()
    public maxPrefix?: [string, boolean];

    @Prop()
    public maxDigits?: number;

    @Prop()
    public minDigits?: number;

    @Prop()
    public isTotal?: boolean;

    @Prop()
    public title?: string;

    @Prop()
    public isLoading?: boolean;

    public get prefix() {
        if (this.maxPrefix === undefined) {
            throw new Error("Missing property: maxPrefix");
        }

        if ((this.value === undefined) || Number.isNaN(this.value)) {
            return "";
        }

        // The following logic is necessary so that negative values will be displayed in alignment with their
        // positive brothers and sisters. In a column with at least one negative value, all positive values are
        // first prefixed with an invisible - sign, before potentially also being prefixed with zeroes and grouping
        // characters.
        const integralValueLength = NumericTableCell.getIntegralPart(this.valueFormatted).length;
        const prefixWithoutSign = this.maxPrefix[0].substr(0, this.maxPrefix[0].length - integralValueLength);

        return ((this.value < 0) || !this.maxPrefix[1]) ? prefixWithoutSign : `${prefixWithoutSign}-`;
    }

    public get valueFormatted() {
        if (this.maxDigits === undefined) {
            throw new Error("Missing property: maxDigits");
        }

        return Format.value(this.value, this.maxDigits, this.minDigits);
    }

    private static readonly decimalSeparator = Format.value(1.1, 1, 1).toString().substr(1, 1);

    private static getIntegralPart(formatted: string) {
        const decimalSeparatorIndex = formatted.indexOf(this.decimalSeparator);

        return decimalSeparatorIndex < 0 ? formatted : formatted.substring(0, decimalSeparatorIndex);
    }
}