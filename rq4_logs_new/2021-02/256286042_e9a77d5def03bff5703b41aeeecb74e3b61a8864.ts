import { Dock } from "@nlpjs/basic";
import { SerializableObject } from '@akala/json-rpc-ws'
import { TrimType } from "@nlpjs/ner";

export default async function positionedEntity(this: { dock: Dock }, locale: string, name: string, srcWords: string[] | string, srcOptions?: SerializableObject)
{
    const options = srcOptions || {};
    const words = Array.isArray(srcWords) ? srcWords : [srcWords];
    const rule = {
        type: TrimType.After,
        words,
        options,
    };
    this.dock.get('ner').addRule(locale, name, 'trim', rule)
}