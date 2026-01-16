
var _doc:Document = typeof document !== 'undefined' ? document : undefined;

export function getDoc():Document {
    return _doc;
}

export function setDoc(value:Document):void {
    _doc = value;
}

export function resetDoc():void {
    _doc = typeof document !== 'undefined' ? document : undefined;
}