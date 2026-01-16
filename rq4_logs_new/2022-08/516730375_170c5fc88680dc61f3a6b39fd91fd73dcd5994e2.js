import { useState } from "react";

const wordByWord = ( sentence ) => {
    const [isFirstTime, setIsFirstTime] = useState(true);
    const [body, setBody] = useState('');
    let index = 0;

    if (isFirstTime) {
        setIsFirstTime(false);

        setTimeout(() => {
            const interval = setInterval(() => {
                setBody( prevState => prevState + sentence[index++].name);

                if (index === sentence.length) {
                    clearInterval(interval)
                }
            }, 80);
        }, 500)
    }

    return body;
}

export default wordByWord;