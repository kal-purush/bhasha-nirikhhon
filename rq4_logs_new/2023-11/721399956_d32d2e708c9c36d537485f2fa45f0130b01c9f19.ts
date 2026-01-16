interface MyPageSlice {
  personalInfo: UserInfoType
}

type UserInfoType = UserInputType & {
  nickname?: string,
  introduction?: string,
  carNumber?: number,
}