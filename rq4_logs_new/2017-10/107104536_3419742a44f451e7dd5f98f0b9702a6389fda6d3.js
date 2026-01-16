// const addForm = (form) => {
//   return {
//     type: 'ADD_FORM',
//     form
//   }
// }
export const createForm = (form) => {
  return {
    type: 'CREATE_FORM',
    form
  }
}
export const initForm = () => {
  return (dispatch) => {
    const model = {
      'email': 'chris@shiip.io',
      'name_first': 'Chris',
      'name_last': 'Cai',
      'status': 'active',
      'submit': 'Submit Form'
    }
    dispatch(createForm(model))
  }
}