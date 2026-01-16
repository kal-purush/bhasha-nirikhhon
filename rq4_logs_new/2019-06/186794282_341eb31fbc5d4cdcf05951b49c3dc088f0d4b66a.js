import { mapGetters, mapActions } from 'vuex';

export const authComputed = {
  ...mapGetters('auth', ['isLogged', 'getUsername']),
};

export const authMethods = mapActions('auth', ['login', 'logout']);