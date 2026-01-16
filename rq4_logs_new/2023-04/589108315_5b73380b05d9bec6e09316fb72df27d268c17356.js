import React, { useState } from 'react'
import { useSelector, useDispatch } from 'react-redux'
import { decrement, increment, incrementByAmount } from './counterSlice'
// import styles from './Counter.module.css'

export function ReduxCounter() {
    const count = useSelector((state) => state.counterApp.value);
    const dispatch = useDispatch();

    const [inputVal, setInputVal] = useState(5);

    return (
        <div className='row'>
            <h1 className='text-center mb-5'>Counter App using Redux</h1>
            <div className='col-4 text-center blockquote'>
                Counter value: {count}
            </div>
            <div className='col-8'>
                <div className='form-group row'>
                    <label className='col-3'>Enter value: </label>
                    <div className='col-8'>
                        <input
                            type="text"
                            value={inputVal}
                            onChange={(e) => setInputVal(parseInt(e.target.value))}
                            className="form-control" />
                    </div>
                </div>
            </div>
            <div className='mt-2 col-12 text-center'>
                <button
                    aria-label="Increment value"
                    onClick={() => dispatch(increment())}
                    className="btn btn-primary"
                >
                    Increment by 1
                </button>
                <button
                    aria-label="Decrement value"
                    onClick={() => dispatch(decrement())}
                    className="mx-2 btn btn-secondary"
                >
                    Decrement by 1
                </button>
                <button
                    onClick={() => dispatch(incrementByAmount(inputVal))}
                    className="mx-2 btn btn-success">Add Input Value</button>
            </div>
        </div>
    )
}