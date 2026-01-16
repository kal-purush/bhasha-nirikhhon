import { message } from '@/utils/resetMessage';
import { notify } from '@/utils/resetNotify';
import { LS, SS } from '@/utils/storage';

const plugins = {
  message,
  notify,
  ls: LS,
  ss: SS,
}

export default {
  install(Vue) {
    Object.keys(plugins).forEach((key) => {
      Object.defineProperty(Vue.prototype, `$${key}`, {
        value: plugins[key],
        writable: false
      })
    })
  },
}