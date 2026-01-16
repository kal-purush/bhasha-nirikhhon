import AccountType from "../models/AccountType"
import MaritalStatus from "../models/MaritalStatus"
import Location from "../models/Location"

export default interface User {
  _id?: string,
  _username: string,
  _password?: string,
  _firstName?: string,
  _lastName?: string,
  _email: string,
  _profilePhoto?: string,
  _headerImage?: string,
  _accountType?: AccountType,
  _maritalStatus?: MaritalStatus,
  _biography?: string,
  _dateOfBirth?: Date,
  _joined?: Date,
  _location?: Location,
}