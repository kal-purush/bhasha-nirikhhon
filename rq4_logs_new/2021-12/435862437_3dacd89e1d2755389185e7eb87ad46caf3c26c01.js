import React from 'react';
import SingleItemOfList from './SingleItemOfList';
import  {useSelector} from 'react-redux';


export default function  List () {
const list = useSelector(state => state.list.list);
console.log(list)

    return (
        <div className='work-xperince-list'>
            { list.map(item=>{
                 return <SingleItemOfList   dataOfItem={item}  key={`${item.idItem}`}/>
                })}
        </div>
    )
}





