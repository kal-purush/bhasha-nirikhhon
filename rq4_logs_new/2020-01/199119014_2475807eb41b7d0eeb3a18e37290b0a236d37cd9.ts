import { IArgument } from '../definition/iArgument'

/**
 * 引数の型定義
 */
export class ArgumentType {

    /**
     * Double
     */
    public static double = new ArgumentType('Double', '1.0', val => val, val => val)

    /**
     * String
     */
    public static string = new ArgumentType(
        'String',
        'string_test_value',
        val => `'${val}'`,
        val => `"${val}"`)

    /**
     * 全ての型
     */
    public static get allTypes(): ArgumentType[] {
        return [this.double, this.string]
    }

    /**
     * 型名
     */
    public get type(): string {
        return this.typeName
    }

    /**
     * テストに用いる加工なしの値
     */
    public get rowTestValue(): string {
        return this.testValue
    }

    /**
     * BVE5の構文上で引数と認識される形式のテスト値
     */
    public get bve5TestValue(): string {
        return this.toBve5TestValue(this.testValue)
    }

    /**
     * C#上で認識される形式のテスト値
     */
    public get csharpTestValue(): string {
        return this.toCsharpTestValue(this.testValue)
    }

    private constructor(
        private readonly typeName: string,
        private readonly testValue: string,
        private readonly toBve5TestValue: (testValue: string) => string,
        private readonly toCsharpTestValue: (testValue: string) => string
    ) {}

    /**
     * 引数に与えられた型名と一致するか？
     * 型名は大文字小文字＆空白無視で判定します。
     * @param type 型名
     */
    public isType(type: string): boolean {
        return type.trim().replace('?', '').toLowerCase() === this.typeName.toLowerCase()
    }

    /**
     * 引数にテスト値をセットして返します。
     * @param arg iArgument
     */
    public setTestValue(arg: IArgument): IArgument {
        arg.test_value_map_grammar = this.bve5TestValue
        arg.test_value_map_grammar_non_quote = this.rowTestValue
        arg.test_value_csharp = this.csharpTestValue
        return arg
    }
}