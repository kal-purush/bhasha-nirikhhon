import _ from "lodash";

const validateEmail = (email: string) => {
  if (_.isEmpty(email)) return "メールアドレスを入力してください";
  if (/^\S+@\S+\.\S+$/.test(email) === false) return "メールアドレスの形式が正しくありません";
  return false;
};
const validatePassword = (password: string) => {
  if (_.isEmpty(password)) return "パスワードを入力してください";
  if (password.length < 6) return "パスワードは6文字以上で入力してください";
  return false;
};

export { validateEmail, validatePassword };