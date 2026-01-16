let lastRef
function useRef(initRef){
    lastRef=lastRef||initRef
    return {
        current:lastRef
    }
}

export default useRef