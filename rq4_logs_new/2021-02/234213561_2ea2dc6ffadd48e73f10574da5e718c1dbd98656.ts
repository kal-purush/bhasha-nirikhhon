import Permissions from './Permissions'

interface CRUDPermissions extends Permissions {
  [name: string]: {
    create: boolean
    read: boolean
    update: boolean
    delete: boolean
  }
}

export default CRUDPermissions