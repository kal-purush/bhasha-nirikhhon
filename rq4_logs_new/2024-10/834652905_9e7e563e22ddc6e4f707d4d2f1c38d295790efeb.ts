interface GenderListProp {
  id: string;
  name: string;
  value: string;
}

export const genderList: GenderListProp[] = [
  { id: "male", name: "男性", value: "0"},
  { id: "female", name: "女性", value: "1"},
  { id: "other", name: "その他", value: "2"},
];