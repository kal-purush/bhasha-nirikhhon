/* useFormError must be declareted after useForm  */




export const useFormError = (error: any) =>{

    const errors:string[] = []
    const errorKeys:string[] =  Object.keys(error)
    for(let i=0; i<errorKeys.length; i++){
        if(error[errorKeys[i]].type == 'required'){
            errors.push(`${errorKeys[i]} is required`)
        }
        else if(error[errorKeys[i]].type == 'pattern'){
            errors.push(`${errorKeys[i]} is invalid`)
        }        
    }
    const newErrors:string[] = []
    newErrors.push(errors.join(`, `))
    return newErrors
}


