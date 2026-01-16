declare module pages.users.list{
    interface IState{
        UserList: pages.users.details.IUser[],
        SelectedUsers: pages.users.details.IUser[]
    }

    interface IProps extends ReactRouter.RouteComponentProps<any, any>{

    }
}

declare module pages.users.details{
    interface IProps extends ReactRouter.RouteComponentProps<any, any>{

    }
    interface IUser {
        id: number;
        email: string;
        login: string;
        firstName: string;
        lastName: string;
    }
    interface IState{
        UserData: IUser;
    }
}