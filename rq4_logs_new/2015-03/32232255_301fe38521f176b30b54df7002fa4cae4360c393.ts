import main=require('./lib/main');

export import User=main.User;
export import UserConfig=main.UserConfig;

export function manager(options:main.ManagerOptions):main.Manager{
    return new main.Manager(options);
}