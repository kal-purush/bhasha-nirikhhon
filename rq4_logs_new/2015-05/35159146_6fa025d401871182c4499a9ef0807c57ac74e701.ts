module Assert {
    export class Assert implements IAssert {
        that: IAssertThat = AssertThat.call(this);
    }

    function AssertThat(): IAssertThat {
        var object: any = function <T>(item: T): IValueAssertion<T> {
            return new AssertionContainer([item]);
        }
        object.all = AssertAll.call(this);
        object.any = AssertAny.call(this);
        return object;
    }

    function AssertAll(): IAssertAll {
        return function <T>(items: T[]): ISetAssertion<T> {
            return new AssertionContainer(items);
        }
    }

    function AssertAny(): IAssertAny {
        return function <T>(items: T[]): ISetAssertion<T> {
            return new AssertionContainer(items, false);
        }
    }

    class AssertionContainer<T> implements ISetAssertion<T>, IValueAssertion<T> {
        items: T[];

        is: IAssertion<T> = Assertion.call(this);
        are: IAssertion<T> = Assertion.call(this);

        every: boolean;

        constructor(items: T[], every: boolean = true) {
            this.items = items;
            this.every = every;
        }

        assert(func: IFilter<T>, expectation: boolean) {
            var result: boolean = this.every ? this.items.every(func) : this.items.some(func);
            if (result != expectation) {
                throw new Error();
            }
        }
    }

    function Assertion<T>(): IAssertion<T> {
        var object: any = BaseAssertion.call(this, (func: Function) => this.assert(func, true));
        object.not = BaseAssertion.call(this, (func: Function) => this.assert(func, false));
        return object;
    }

    function BaseAssertion<T>(assert: Function): IBaseAssertion<T> {
        return {
            equal: AssertEqual.call(this, assert),
            exact: AssertExact.call(this, assert),
            different: AssertDifferent.call(this, assert),
            distinct: AssertDistinct.call(this, assert),

            greater: AssertGreater.call(this, assert),
            less: AssertLess.call(this, assert),

            true: () => { assert(item => item) },
            false: () => { assert(item => !item) },
            assigned: () => { assert(item => item != null) },
            null: () => { assert(item => item == null)},
            undefined: () => { assert(item => item == undefined) },
            defined: () => { assert(item => item != undefined) },

            match: AssertMatch.call(this, assert)
        };
    }

    function AssertEqual<T>(assert: Function): IAssertEqual<T> {
        return { to: <T>(item: T) => { assert(value => value == item); } };
    }

    function AssertExact<T>(assert: Function): IAssertExact<T> {
        return { to: <T>(item: T) => { assert(value => value === item); } };
    }

    function AssertDifferent<T>(assert: Function): IAssertDifferent<T> {
        return { from: <T>(item: T) => { assert(value => value != item); } };
    }

    function AssertDistinct<T>(assert: Function): IAssertDistinct<T> {
        return { from: <T>(item: T) => { assert(value => value !== item); } };
    }

    function AssertGreater<T>(assert: Function): IAssertGreater<T> {
        return { than: <T>(item: T) => { assert(value => value > item); } };
    }

    function AssertLess<T>(assert: Function): IAssertLess<T> {
        return { than: <T>(item: T) => { assert(value => value < item); } };
    }

    function AssertMatch<T>(assert: Function): IAssertMatch<T> {
        return {
            expression: (item: RegExp) => { assert(value => item.test(value)); },
            test: <T>(func: IFilter<T>) => { assert(value => func(value)); }
        };
    }

    export interface IAssert {
        that: IAssertThat;
    }

    interface IAssertThat {
        <T>(item: T): IValueAssertion<T>;
        all: IAssertAll;
        any: IAssertAny;
    }

    interface IAssertAll {
        //<T>(a: T, ...items: T[]): ISetAssertion<T>;
        <T>(items: T[]): ISetAssertion<T>;
    }

    interface IAssertAny {
        //<T>(a: T, ...items: T[]): ISetAssertion<T>;
        <T>(items: T[]): ISetAssertion<T>;
    }

    interface IValueAssertion<T> {
        is: IAssertion<T>;
    }

    interface ISetAssertion<T> {
        are: IAssertion<T>;
    }

    interface IBaseAssertion<T> {
        equal: IAssertEqual<T>;
        exact: IAssertExact<T>;
        different: IAssertDifferent<T>;
        distinct: IAssertDistinct<T>;

        greater: IAssertGreater<T>;
        less: IAssertLess<T>;

        true: () => void;
        false: () => void;
        assigned: () => void;
        null: () => void;
        undefined: () => void;
        defined: () => void;

        match: IAssertMatch<T>;
    }

    interface IAssertion<T> extends IBaseAssertion<T> {
        not: IBaseAssertion<T>;
    }

    interface IAssertEqual<T> {
        to: IAssertTo<T>;
    }

    interface IAssertExact<T> {
        to: IAssertTo<T>;
    }

    interface IAssertTo<T> {
        (item: T): void;
    }

    interface IAssertDifferent<T> {
        from: IAssertFrom<T>;
    }

    interface IAssertDistinct<T> {
        from: IAssertFrom<T>;
    }

    interface IAssertFrom<T> {
        (item: T): void;
    }

    interface IAssertGreater<T> {
        than: IAssertThan<T>;
    }

    interface IAssertLess<T> {
        than: IAssertThan<T>;
    }

    interface IAssertThan<T> {
        (item: T): void;
    }

    interface IAssertMatch<T> {
        expression: IAssertRegex;
        test: IAssertTest<T>
    }

    interface IFilter<T> {
        (item: T): boolean;
    }

    interface IAssertTest<T> {
        (filter: IFilter<T>): void;
    }

    interface IAssertRegex {
        (regex: RegExp): void;
    }
} 