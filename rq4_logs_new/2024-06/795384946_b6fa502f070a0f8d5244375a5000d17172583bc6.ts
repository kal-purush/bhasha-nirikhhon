import { useEffect, useRef, useState } from "react";

export function useInterval(callback: () => any, delay: number) {

    const [enabled, setEnabled] = useState(true);
    // let enabled = true;
    // const setEnabled = (e: boolean) => {enabled = e;}

    const savedCallback = useRef<typeof callback>();

    const onVisChange = () => {
        if(document.hidden) {
            if(enabled) {
                setEnabled(false);
                console.log("Tab is now hidden. Disabling useInterval");
            }
        }
        else {
            if(!enabled) {
                setEnabled(true);
                console.log("Tab is now in focus. Enabling useInterval");
            }
        }
    };

    // Remember the latest callback.
    useEffect(() => {
        document.addEventListener("visibilitychange", onVisChange);

        savedCallback.current = callback;

        onVisChange();
        
        // Specify how to clean up after this effect:
        return () => {
            document.removeEventListener("visibilitychange", onVisChange);
        };

    }, [callback]);

    // Set up the interval.
    useEffect(() => {
        function tick() {
            console.log("tick ended. enabled",enabled);
            if(enabled) savedCallback.current ? savedCallback.current() : (() => {})()
        }
        if (delay !== null) {
          let id = setInterval(tick, delay);
          return () => clearInterval(id);
        }
      }, [delay, enabled]);
}