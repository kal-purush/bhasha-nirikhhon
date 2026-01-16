import { Vue } from "nuxt-property-decorator"
import _ from "lodash"

export abstract class SettingValueEditorBase<T> extends Vue {
  abstract value:T

  original_value:T|null = null

  /**
   * 2020-07-07 14:51
   * 
   * At least one and REQUIRED validation. Check whether the value is the same or not.
   * Setting value data structure are different for all service settings and are usually
   * more complex than a primary string. So this is required.
   */
  abstract validateBeforeRequestImpl(new_value:T):string|void
  validateBeforeRequest(new_value:T):string|void {
    if(this.original_value != null) {
      const result = this.validateIsSame()
      if(result != undefined) return result
    }

    this.validateBeforeRequestImpl(new_value)
  }
  validateIsSame():void|string {
    const value_type = typeof this.original_value
    let is_same = false
    if(value_type == "string") {
      is_same = this.original_value == this.value
    }
    else if(value_type == "object") {
      is_same = Object.entries(this.original_value!).every(([key, value]) => _.isEqual((<any>this.value)[key], value))
    }

    if(is_same) {
      return "Value is the same."
    }
  }
  abstract renderValidationError(validation_error:any):void
  
  loadValue(value:T):void {
    this.original_value = _.cloneDeep(value)
    this.value = _.cloneDeep(value)
  }
}