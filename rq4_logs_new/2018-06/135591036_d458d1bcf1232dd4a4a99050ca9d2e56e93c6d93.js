function fibonacci(position) {
    if(position < 3) return 1
    else return fibonacci(position - 1) + fibonacci(position - 2)
}

//console.log(fibonacci(20));

function memmorizedFiboacci(index, cache) {
    cache = cache || [];
    if(cache[index]) return cache[index]
    else {
        if(index < 3) return 1;
        else {
            cache[index] = memmorizedFiboacci(index - 1, cache) + memmorizedFiboacci(index - 2, cache);
        }
    }
    return cache[index];   
}

console.log(memmorizedFiboacci(3));