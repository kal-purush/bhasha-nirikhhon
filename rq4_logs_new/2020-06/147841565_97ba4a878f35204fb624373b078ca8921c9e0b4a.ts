import { VuexModule, Module, Action, Mutation, getModule } from 'vuex-module-decorators'

import User from "@ctsy/api-sdk/dist/modules/User";
import hook, { HookWhen, Hook } from '@ctsy/hook';
import { VuexHooks } from '..';

@Module({})
export default class auth extends VuexModule {

    User: { UID: 0 } | any = { UID: 0 };
    @Mutation
    protected set_user(user: any) {
        this.User = user;
        hook.emit(VuexHooks.LoginSuccess, HookWhen.After, this, user);
    }
    /**
     * 登录操作
     * @param data d
     */
    @Action({ rawError: true })
    async login(data: { username: string, password: string }) {
        let rs = await User.AuthApi.login(data.username, data.password);
        if (rs.UID && rs.UID > 0) {
            this.context.commit('set_user', rs);
            return rs;
        }
        return this.User;
    }

    @Action({ rawError: true })
    async relogin() {
        let rs = await User.AuthApi.relogin();
        if (rs.UID && rs.UID > 0) {
            this.context.commit('set_user', rs);
            return rs;
        }
        return this.User;
    }
    /**
     * 退出登录
     */
    @Action({ rawError: true })
    async logout() {
        this.context.commit('set_user', { UID: 0 });
        await hook.emit(VuexHooks.Logout, HookWhen.Before, this, {});
        let rs = await User.AuthApi.logout();
        await hook.emit(VuexHooks.Logout, HookWhen.After, this, {});
        return this.User;
    }
}