import useState from 'react'; 


export const useForm = (intialValue)=>{
    const [value,setValue] = useState(initValues); 
    return [ 
        value,e=>{
        setValue({
            ...value,
            [e.targe.name]:[e.target.value]
        })
        }
    ]    
}