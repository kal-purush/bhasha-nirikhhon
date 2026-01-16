import React, { Fragment } from 'react'
import { useForm } from '../../hooks/useForm';

export const TodoAdd = ({handleAddTodo}) => {
    const [{ desc }, handleInputChanges, reset] = useForm({
        desc: ''
    });

    const handleSubmit=(e)=>{
        e.preventDefault();
        if (desc.trim().length <= 1) {
            return;
        }
        const newTodo = {
            id: new Date().getTime(),
            desc,
            done: false
        };
        handleAddTodo(newTodo);
        reset();
    }

    return (
        <Fragment>
            <h4>Agregar TODO</h4>
            <hr />
            <form onSubmit={handleSubmit}>
                <input
                    type='text'
                    name='desc'
                    className='form-control'
                    placeholder="Inserte un todo"
                    autoComplete='off'
                    value={desc}
                    onChange={handleInputChanges}
                />
                <button
                    type='submit'
                    className="btn btn-outline-primary mt-1 btn-block"
                >
                    Agregar
                            </button>
            </form>
        </Fragment>
    )
}