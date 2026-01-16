interface Profile {
  id: number
  name: string
  age: number
  avatar: string
  user_id: number
}

type Profiles = Profile[]

interface User {
  id: number
  email: string
  password: string
  first_name: string
  last_name: string
  date_created: string
  profiles: Profiles
}

export interface AccountInformationProps {
  user: User
}

export interface EditAccountProps {
  user: User
}

export interface EditAccountInformationProps {
  user: User
}

export interface EditProfileProps {
  profiles: Profiles
  handleSwitchProfile: (profileID: number) => void
  isModal: boolean
}

export interface ModalEditProfileProps {
  profile: Profile
  visible: boolean
  confirmLoading: boolean
  handleOk: (username: String) => void
  handleCancel: () => void
}

export interface ModalSwitchProfileProps {
  visible: boolean
  handleSwitchProfile: (profileID: number) => void
  handleCancel: () => void
  profiles: Profiles
}

export interface TopBarComponentProps {
  visible: boolean
  setVisible: (visible: boolean) => void
  profiles: Profiles
  handleCancel: () => void
  handleSwitchProfile: (profileID: number) => void
  handleLogout: () => void
}

export interface TopMenuProps {
  visible: boolean
  setVisible: (visible: boolean) => void
  profiles: Profiles
  handleCancel: () => void
  handleSwitchProfile: (profileID: number) => void
  handleLogout: () => void
}