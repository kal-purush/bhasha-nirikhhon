import React, { useState } from "react"

function Counter() {
    const [count, setCount] = useState(0)
    return (
        <>
            <h2>useState</h2>
            <h3>Count {count}</h3>
            { /* Note: don't operate on count directly, e.g. count++ */ }
            <button onClick={() => setCount(count - 1)}>Decrement (-)</button>&nbsp;
            <button onClick={() => setCount(count + 1)}>Increment (+)</button>&nbsp;
        </>
    )
}

export default function UseStateDemo() {
    return <Counter />
}