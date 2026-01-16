class Example {
    isString(value) {
        return Object.prototype.toString.call(value) === '[object String]'
    }

    reverseString(value) {
        if (this.isString(value)) {
            return value
                .split('')
                .reverse()
                .join('')
        } else {
            throw new Error('reverseString may only be called with a String')
        }
    }
}

module.exports = new Example()