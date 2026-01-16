import { alt } from "../../shared/alt";
import { FetchActions } from "../../shared/actions/FetchActions";
import { User } from "../../shared/models/Generated";

interface Actions {
    setCurrentUser(username: string): string;
    beginFetchUsers() : boolean;
    updateUsers(users: User[]) : User[]
}

class UserActions extends FetchActions<User[]> implements Actions {

    setCurrentUser(username: string): string {
        return username;
    }

    updateUsers(data: User[]) {
        return data;
    }

    beginFetchUsers(): boolean {
        return true;
    }

}

export const userActions =
    alt.createActions<Actions>(UserActions);