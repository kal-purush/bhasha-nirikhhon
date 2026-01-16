import React, { useEffect } from 'react';
import { useDispatch, useSelector } from 'react-redux'
import { displayGreeting } from './redux/greetingsReducer'

const Greeting = () => {
  const dispatch = useDispatch();
  const greeting  = useSelector((state) => state.greeting);
  useEffect(() => {
    dispatch(displayGreeting());
  }, [dispatch]);

  return (
    <div>
    {greeting.map((elem) => (
      <div key={elem.id}>
      <i class="fas fa-sun"></i>
      <p>{elem.message}</p>
      </div>
    ))}
    </div>
  );
};

export default Greeting;