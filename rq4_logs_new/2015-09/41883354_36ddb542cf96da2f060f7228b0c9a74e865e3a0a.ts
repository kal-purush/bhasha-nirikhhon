'use strict';

interface IMock<T> extends IMockSetupMany<T> {
    setup(methodSelector: (obj: T) => any): jasmine.Spy;
    getObject(): T;
    getSpy(methodSelector: (obj: T) => any): jasmine.Spy
    verifyAll(): void;
}

interface IMockSetupMany<T> {
    setupMany(methodSelector: (obj: T) => any): IMockSetupMany<T>;
}

class Mock<T> implements IMock<T>{
    private object: T;
    private spies: amp.models.IMap<jasmine.Spy> = {};
    constructor() {
        this.object = <T><any>{
            __mockParent: this,
        };
    }

    public static getMockFromObject<T>(mockedObject: T): Mock<T> {
        return <Mock<T>>(<any>mockedObject).__mockParent;
    }

    public setup(methodSelector: (obj: T) => any): jasmine.Spy {
        var method = this.getMethod(methodSelector);
        this.object[method] = (args) => args;
        var spy = spyOn(this.object, method);
        this.spies[method] = spy;
        return spy;
    }

    public setupMany(methodSelector: (obj: T) => any): IMockSetupMany<T> {
        this.setup(methodSelector);
        return this;
    }

    public getSpy(methodSelector: (obj: T) => any): jasmine.Spy {
        var method = this.getMethod(methodSelector);
        return this.spies[method];
    }

    public getMethod(methodSelector: (T) => any): string {
        var method = methodSelector.toString();
        var regex = new RegExp('\\.([^( ;]+)');
        var match = regex.exec(method);
        if (!match) {
            throw new Error('Lambda isn\'t a method/property expresion');
        }
        return match[1];
    }

    public verifyAll(): void {
        for (var prop in this.spies) {
            var spy: jasmine.Spy = this.spies[prop];
            expect(spy).toHaveBeenCalled();
        }
    }

    public getObject(): T {
        return this.object;
    }
}