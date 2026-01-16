import React, {useState} from 'react';
import _ from 'lodash';

/**
 * also:
 *  - validations, 
 *    - each can decide if it wants to validate onChange and/or onSubmit
 *    - Use rails validations
 *  - user supplied callback for onSubmit
 *  - user supplied callback for onChange
 *  - user supplied callback for form submission response.
 *  - state if form is submitting
 * 
 */

const useForm = <IFields>(initialValues: IFields) => {

  const [fields, setFields] = useState(initialValues)

  function handleOnSubmit(e: React.FormEvent<HTMLFormElement>) {
    // user supplied callback
    e.preventDefault();
    console.log('form was submitted!', fields);
  }


  function handleFieldChange(e: React.FormEvent<HTMLInputElement>) {
    const {value, name} = e.currentTarget
    const newFields = Object.assign({}, fields, {[name]: value})
    console.log("TCL: handleFieldChange -> newFields", newFields)
    setFields(newFields)
  }

  return {
    handleOnSubmit,
    handleFieldChange,
    fields
  }
}

export default useForm