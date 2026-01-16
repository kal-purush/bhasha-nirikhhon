import {useState, useEffect, useRef} from 'react';

import {TUseTypingText} from '../model';

import {DEFAULT_PERIOD, DEFAULT_DELTA_PROCENT, DEFAULT_DELTA_THEN_IS_EMPTY} from './constants';

export const useTypingText = ({erasingAndTypingTime, texts}: TUseTypingText) => {
    const [typingText, setTyping] = useState('');
    const isDeleting = useRef(false);
    const loopNum = useRef(0);
    const newText = useRef('');
    const timeoutId = useRef<number>();

    const tick = () => {
        let delta = erasingAndTypingTime - Math.random() * DEFAULT_DELTA_PROCENT;
        const i = loopNum.current % texts.length;
        const fullTxt = texts[i];

        setTyping(prevText => {
            if (isDeleting.current) {
                newText.current = fullTxt.substring(
                    0,
                    // 2-ка стирается/добавляется только для иконок
                    // т.к. иконка состоит из 2х символов
                    prevText.length - (fullTxt.length - prevText.length > 2 ? 1 : 2)
                );

                delta /= 2;

                if (newText.current === '') {
                    isDeleting.current = false;
                    loopNum.current += 1;
                    delta = DEFAULT_DELTA_THEN_IS_EMPTY;
                }

                return newText.current;
            }

            newText.current = fullTxt.substring(
                0,
                prevText.length + (fullTxt.length - prevText.length > 2 ? 1 : 2)
            );

            if (newText.current === fullTxt) {
                delta = DEFAULT_PERIOD;
                isDeleting.current = true;
            }

            return newText.current;
        });

        timeoutId.current = window.setTimeout(() => {
            tick();
        }, delta);
    };

    const stopTyping = () => {
        timeoutId.current && window.clearTimeout(timeoutId.current);
    };

    useEffect(() => {
        // tick должен сработать 1 раз при рендере
        stopTyping();
        tick();

        return () => {
            stopTyping();
        };

    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [texts, erasingAndTypingTime]);

    return {typingText, stopTyping};
};