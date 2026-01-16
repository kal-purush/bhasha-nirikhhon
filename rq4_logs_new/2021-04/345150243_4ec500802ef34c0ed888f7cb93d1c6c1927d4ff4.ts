export class FormHelper {
  static ToJson = (form: any) => {
    const payload = Object.keys(form).reduce((payload, key) => {
      let value: string | boolean;

      if (form[key].type == 'bool') {
        value = form[key].checked
      }
      else {
        value = form[key].value
      }

      return { ...payload, [key]: value }
    }, {})

    return payload
  }

  static SetValue = (form: any, name: any, value: any) => {
    if (form[name].type == 'bool') {
      form[name].checked = value
    }
    else {
      form[name].value = value
    }

    return form
  }

  static Clear = (form: any) => {
    Object.keys(form).forEach(name => {
      if (form[name].type == 'bool') {
        form[name].checked = false
      }
      else {
        form[name].value = ''
      }
    })

    return form
  }
}