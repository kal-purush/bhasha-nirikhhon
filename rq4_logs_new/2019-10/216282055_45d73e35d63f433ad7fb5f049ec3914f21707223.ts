import React, {useState} from 'react';

import _ from 'lodash';
/**
 * also:
 *  - validations,
 *    - on submit validation
 *    - each can decide if it wants to validate onChange and/or onSubmit
 *    - Use rails validations
 *  - user supplied callback for onSubmit which fires before submitting
 *  - user supplied callback for form submission response which provides the submit result
 *  - state if form is submitting
 * 
 */

// the interface to represent fields
interface BasicFields {
  [fieldName: string]: {
    value: string,
    validateOnSubmit: boolean
  }
}

// interface to represent the state of each field
interface IFieldState {
  [fieldName: string]: {
    value: string,
    errors: string[]
  }
}

const useForm = <IFields extends BasicFields>(
  initialFields: IFields,
  submitCb: (e: React.FormEvent<HTMLFormElement>, fields: IFieldState) => void
) => {

  const [fields, setFields] = useState(() => {
    // transform config object representing fields to a slimmer object mapping 
    return _.transform(initialFields, (result, fieldData, fieldName) => {
      result[fieldName] = {value: fieldData.value, errors: []}
    }, {} as IFieldState)
  })

  function handleOnSubmit(e: React.FormEvent<HTMLFormElement>) {
    // user supplied callback
    e.preventDefault();
    validateSubmit()

    if (canSubmit()) {
      submitCb(e, fields)
    } else {
      console.log('form is not valid for sumbission');
    }
  }

  /**
   * run onSubmit validations
   */
  function validateSubmit(): void {
    _.each(fields, (fieldData) => {
      if (!fieldData.value) {
        fieldData.errors = ['field is required'];
      } else {
        fieldData.errors = [];
      }
    })
  }

  /**
   * form is in a valid state for submision
   */
  function canSubmit(): boolean {
    const errors = _.reduce(fields, (result, value, key) => result.concat(value.errors), [] as string[])
    return errors.length === 0
  }

  function handleFieldChange(e: React.FormEvent<HTMLInputElement>) {
    const {value, name} = e.currentTarget
    const newFields = _.cloneDeep(fields)
    newFields[name].value = value
    setFields(newFields)
  }

  return {
    handleOnSubmit,
    handleFieldChange,
    fields
  }
}

export default useForm