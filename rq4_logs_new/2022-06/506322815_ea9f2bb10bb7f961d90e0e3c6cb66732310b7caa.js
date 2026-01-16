import './charactercard.css'
import { useState } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { addFevourite, removeFevourite, } from '../../Redux files/actions';
import { Card } from '@mui/material';
import StarIcon from '@mui/icons-material/Star';


function CharacCard({
  charac
}) {
  const favData = useSelector(state => state)
  const [isFavourite, setIsFavourite] = useState((charac in favData) ? true : false)
  const dispatch = useDispatch()

  const HandleAdd = () => {
    setIsFavourite(true)
    dispatch(addFevourite(charac))
    console.log(favData)
  }

  const HandleRemove = () => {
    setIsFavourite(false)
    dispatch(removeFevourite(charac))
  }

  return (
    <div>
      <div>
        <Card variant="outlined" class='card'>
          <img src={charac.image}></img>
          <h4>Id:{charac.id}</h4>
          <h4>Name:{charac.name}</h4>
          <h4>Status:{charac.status}</h4>
          {(isFavourite) ?
            <StarIcon className='favorite' onClick={HandleRemove}></StarIcon> :
            <StarIcon onClick={HandleAdd} ></StarIcon>
          }

        </Card>
      </div>
    </div>
  )
}

export default CharacCard;