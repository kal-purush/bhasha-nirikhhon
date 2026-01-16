/// <reference path="Fragment.ts" />

module Sanara.Core{
    export type DictionaryEntry = Sanara.Core.FragmentClass// | Sanara.Core.Transformer;
    export class Dictionary {
        constructor(private entries : DictionaryEntry[]) {}
        hasEntry(name:string) : boolean {
            for(var i = 0 ; i < this.entries.length ; i++) {
                if(this.entries[i].doc.name === name){
                    return true;
                }
            }
            return false;
        }
        getEntry(name:string) : Sanara.Core.DictionaryEntry {
            for(var i = 0 ; i < this.entries.length ; i++) {
                if(this.entries[i].doc.name === name){
                    return this.entries[i];
                }
            }
            return null;
        }
        listNames() : string[] {
            var list = <string[]>[];
            for(var i = 0 ; i < this.entries.length ; i++) {
                list.push(this.entries[i].doc.name)
            }
            return list;
        }
    }
}