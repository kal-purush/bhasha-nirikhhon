
// https://smellegantcode.wordpress.com/2015/04/02/typescript-1-5-get-the-decorators-in/
class C {
	@log
    foo(n: number) {
        return n * 2;
    }
}
function log(target: Function, key: string, value: any) {
  console.log(target,key,value);
    return {
        value: (...args: any[]) => {

            var a = args.map(a => JSON.stringify(a)).join();
            var result = value.value.apply(this, args);
            var r = JSON.stringify(result);

            console.log(`Call: ${key}(${a}) => ${r}`);

            return result;
        }
    };
}
var c = new C();
console.log(c.foo(23));