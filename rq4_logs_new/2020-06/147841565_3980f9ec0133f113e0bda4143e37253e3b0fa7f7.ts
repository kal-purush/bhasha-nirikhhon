/*
 *
 *公共正则
 */

namespace PublicReg {
  class defaultReg {
    name: RegExp = /^[\u4E00-\u9FA5]{2,15}$/;
    nick: RegExp = /^[\u4E00-\u9FA5]{2,10}$/;
    account: RegExp = /^([^$@$!%*#?&])([A-Za-z0-9$@$!%*#?&]){6,13}$/;
    phone: RegExp = /^[+86]{0,}1\d{10}$/;
    messageCode: RegExp = /^\w{4,}$/;
    email: RegExp = /^\w+@[a-z0-9]+(\.[a-z]+){1,3}/;
    emailCode: RegExp = /^\w{4,}$/;
    pwd: RegExp = /^(?![0-9]+$)(?![a-zA-Z]+$)[0-9A-Za-z]{6,12}$/;
  }
  /**
   * 传入自定义正则
   */
  export class CustomReg {
    /**
     * 姓名
     */
    name?: RegExp;
    /**
     * 昵称
     */
    nick?: RegExp;
    /**
     * 账号
     */
    account?: RegExp;
    /**
     * 电话号码
     */
    phone?: RegExp;
    /**
     * 短信验证码
     */
    messageCode?: RegExp;
    /**
     * 邮箱
     */
    email?: RegExp;
    /**
     * 邮箱验证码
     */
    emailCode?: RegExp;
    /**
     * 密码，重复密码与密码共用
     *      */
    pwd?: RegExp;
  }
  export const Reg = new CustomReg();
  const DefaultRegExp = new defaultReg();
  export class Regs {
    /**
     * 中文姓名  根据资料查询，中国人最长的中文名称为15
     */
    name = DefaultRegExp.name || Reg.name;
    /**
     * 中文昵称
     */
    nick = DefaultRegExp.nick || Reg.nick;
    /**
     * 账号 不能以特殊字符开头，可以输入特殊字符
     */
    account = DefaultRegExp.account || Reg.account;
    /**
     * 电话号码
     */
    phone = DefaultRegExp.phone || Reg.phone;
    /**
     * 短信验证码
     */
    messageCode = DefaultRegExp.messageCode || Reg.messageCode;
    /**
     * 邮箱
     */
    email = DefaultRegExp.email || Reg.email;
    /**
     * 邮箱验证码
     */
    emailCode = DefaultRegExp.emailCode || Reg.emailCode;
    /**
     * 密码必须包含数字字母
     */
    pwd = DefaultRegExp.pwd || Reg.pwd;
  }
}

export default PublicReg;