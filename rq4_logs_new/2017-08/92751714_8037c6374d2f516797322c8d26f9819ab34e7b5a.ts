export function logMethod(target: Object, key: string, descriptor: TypedPropertyDescriptor<any>) {
    var originalMethod = descriptor.value;

    //editing the descriptor/value parameter
    descriptor.value = function (...args: any[]) {
        console.log('logMethodxxxxxxxxx', target, key, descriptor);

        var a = args.map(a => JSON.stringify(a)).join();
        // note usage of originalMethod here
        var result = originalMethod.apply(this, args);
        var r = JSON.stringify(result);
        console.log(`Call: ${key}(${a}) => ${r}`);
        return result;
    }

    // return edited descriptor as opposed to overwriting 
    // the descriptor by returning a new descriptor
    return descriptor;
}


export function logMethod2(): MethodDecorator {

    return (target: Object, key: string, descriptor: TypedPropertyDescriptor<any>) => {
        console.log('logMethod22222222a');
        const originalMethod = descriptor.value;
        descriptor.value = function () {
            console.log('logMethod22222222b');
            return originalMethod.apply(this, arguments);
        }
        return descriptor;
    }
}


export function logMethod3(): MethodDecorator {

    return function (target: Object, key: string, descriptor: TypedPropertyDescriptor<any>) {

        const originalMethod = descriptor.value as Function;
        descriptor.value = function () {
            console.log('logMethod333333', descriptor);
            console.log('xxxxxxxxxxxxxx', descriptor.value);
            return originalMethod.apply(this, arguments);
            // Function.apply(), stuff originalMethod function to 'this' object
            // Return is needed here must return what original fn returns, not like setTimeout one
        }
        return descriptor;
    }
}