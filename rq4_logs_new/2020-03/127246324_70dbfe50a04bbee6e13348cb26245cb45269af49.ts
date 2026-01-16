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

import { AssetTypeName } from "./validation/schemas/AssetTypeName.schema";
import { AssetUnion } from "./validation/schemas/AssetUnion.schema";
import { IOrdering } from "./Ordering";

/** @internal */
export interface IParent {
    readonly assets: {
        readonly ordering: IOrdering;
        readonly grandTotalValue?: number;
    };

    readonly exchangeRate?: number;
}

export interface IEditable {
    readonly type: AssetTypeName | "";
    readonly parent: IParent;
    toJSON(): AssetUnion;
}