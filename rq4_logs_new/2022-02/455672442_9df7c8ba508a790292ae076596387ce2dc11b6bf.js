import React, { createContext, useContext, useState } from "react";

import data from '../store/data.json';

const Context = createContext();

const ContextProvider = ({children}) => {
  const [exercises, setExercises] = useState(data.exercises);

  const addExercise = (title, details) => {
    const maxId = Math.max(...exercises.map((exerc) => exerc.id));
    setExercises(prevValue => [...prevValue, {id: maxId+1, title, details, completed: false}])
  };

  const editExercise = (item) => {
    let filteredExercises = [...exercises.filter(i => parseInt(i.id) !== parseInt(item.id)), item]
    setExercises(filteredExercises)
  }

  return (
    <Context.Provider value={{ exercises, addExercise, setExercises, editExercise }}>
      {children}
    </Context.Provider>
  );
};

export const useExerciseContext = () => {
    const {exercises, addExercise, setExercises, editExercise} = useContext(Context);
    return ({
        exercises, addExercise, setExercises, editExercise
    })
}

export default ContextProvider;