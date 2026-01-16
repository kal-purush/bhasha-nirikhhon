import Title from '@/components/title'
import { useState } from 'react';

export default function Homepage(){
    const[count, setCount] = useState(0)
    const countMe = () =>{
        setCount(count + 1) 
    }

    return(
        <>
        <div>
            <p className={`font-${count <5 && "bold"} underline`}>HOMEPAGE</p>
            <p>{count}</p>
            <button className="flex items-center justify-center font-bold text-orange-800 underline drop-shadow"onClick={() => setCount(count + 1 )}>Hit me</button>
        </div>
        <div>
            <Title name="A" school="B" mall="C"/>
        </div>
        </>
    );
}

