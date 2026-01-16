import React, { useEffect } from 'react'
import { useDispatch, useSelector } from 'react-redux';
import { getCuisines } from '../../redux/actions/cuisinesAction';

export default function CuisineItem({ name, img }) {
  const cuisines = useSelector(state => state.cuisines);
  console.log('cuisines --->', cuisines);

  return (
    <div>CuisineItem</div>
  )
}
