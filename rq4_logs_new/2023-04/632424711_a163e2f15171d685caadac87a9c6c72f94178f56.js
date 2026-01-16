const doAfter = (seconds) => {
    const myPromise = new Promise((res, rej) => {
        setTimeout(() => {
            const randValue = Math.random()
            if (randValue > 0.5) {
                res(randValue)
            } else {
                rej(randValue)
            }

        }, seconds * 1000)

    });

    return myPromise
}