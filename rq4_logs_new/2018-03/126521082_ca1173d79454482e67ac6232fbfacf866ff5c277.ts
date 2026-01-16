"use strict";

import { OrderedSet, Map } from "immutable";
import * as handlebars from "handlebars";

import {
    TypeKind,
    Type,
    EnumType,
    UnionType,
    ClassType,
    matchType,
    nullableFromUnion,
    removeNullFromUnion,
    directlyReachableSingleNamedType
} from "quicktype/dist/Type";
import { TypeGraph } from "quicktype/dist/TypeGraph";
import { Sourcelike, maybeAnnotated, modifySource } from "quicktype/dist/Source";
import {
    utf16LegalizeCharacters,
    utf16StringEscape,
    splitIntoWords,
    combineWords,
    firstUpperWordStyle,
    camelCase
} from "quicktype/dist/Strings";
import { intercalate, defined, assert, panic, StringMap } from "quicktype/dist/Support";
import { Name, DependencyName, Namer, funPrefixNamer } from "quicktype/dist/Naming";
import { ConvenienceRenderer, ForbiddenWordsInfo } from "quicktype/dist/ConvenienceRenderer";
import { TargetLanguage } from "quicktype/dist/TargetLanguage";
import { StringOption, EnumOption, Option } from "quicktype/dist/RendererOptions";
import { anyTypeIssueAnnotation, nullTypeIssueAnnotation } from "quicktype/dist/Annotation";
import { StringTypeMapping } from "quicktype/dist/TypeBuilder";

const unicode = require("unicode-properties");

export type Version = 5 | 6;
export type OutputFeatures = { helpers: boolean; attributes: boolean };

export enum AccessModifier {
    None,
    Public,
    Internal
}

export default class CSharpEntityClassTargetLanguage extends CSharpTargetLanguage {
    protected get rendererClass(): new (
        graph: TypeGraph,
        leadingComments: string[] | undefined,
        ...optionValues: any[]
    ) => ConvenienceRenderer {
        return CSharpEntityClassRenderer;
    }
}

export class CSharpEntityClassRenderer extends CSharpRenderer {
    
}