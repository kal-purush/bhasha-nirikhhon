import {
  getTodoList,
} from '../api/api';

export const getTodo = ({
  commit,
}) => new Promise((resolve) => {
  /**
   *调用 getTodo这个函数的时候
   会调用getTodoList这个ajax请求函数，
   函数返回值后，在调用store.js里面的EDITTODE方法，并且把值传给它。
   */
  getTodoList().then((res) => {
    commit('EDITTODE', res.data.todos);
    resolve();
  });
});

export const updateMenu = ({
  commit,
}) => {
  commit('MENUOPEN');
};