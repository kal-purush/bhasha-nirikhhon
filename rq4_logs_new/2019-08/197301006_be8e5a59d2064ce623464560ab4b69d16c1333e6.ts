// declare var Symbol.asyncIterator: symbol;

interface SymbolConstructor {
    readonly asyncIterator: symbol;
}

if (!Symbol.asyncIterator) {
    Object.defineProperty(Symbol, 'asyncIterator', {
        value: Symbol('Symbol.asyncIterator'),
    });
}