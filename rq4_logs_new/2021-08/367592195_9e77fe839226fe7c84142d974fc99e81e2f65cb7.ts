import { WithUid } from "../models/common-models";
import { find, save } from "../data-services/firebase-services";
import { useEffect } from "react";
import { useRecoilState } from "recoil";
import { log } from "../util/logging-config";
import { userState } from "../atoms/user-atoms";
import { UserData } from "../models/user-models";
import { collection as usersCollection } from "./use-users";
import { useFirebaseUser } from "./use-firebase-user";
import { newDocument } from "../mappings/document-mappings";
import firebase from "firebase";
import set from "lodash/set";
import cloneDeep from "lodash/cloneDeep";

export default function useCurrentUser() {
    const {user} = useFirebaseUser();
    const auth = firebase.auth();
    const [document, setDocument] = useRecoilState<WithUid<UserData>>(userState);

    useEffect(() => {
        if (user?.uid) {
            log.debug("User change:", document);
            find<UserData>(usersCollection, user.uid).then((response: WithUid<UserData>) => {
                log.debug("response was:", response);
                setDocument(response);
            });
        }
    }, [user]);

    useEffect(() => {
        log.debug("User change:", document);
    }, [document]);


    function saveUser(changeType: string): Promise<string> {
        return save<UserData>(usersCollection, document).then((response) => {
            log.debug("response was:", response);
        }).then(() => `${changeType} changes were saved successfully.`);
    }

    function createUser(firstName: string, lastName: string) {
        const newUser = newDocument<UserData>();
        newUser.uid = user.uid;
        newUser.createWithId = true;
        newUser.data.firstName = firstName;
        newUser.data.lastName = lastName;
        save<UserData>(usersCollection, newUser).then((response) => {
            log.debug("response was:", response, "created new user:", newUser);
        });
    }

    function mutableUserData(): WithUid<UserData> {
        return cloneDeep(document);
    }

    function changeField(field: string, value: any) {
        log.debug("change:field:", field, "value:", value, "typeof:", typeof value);
        const mutable = mutableUserData();
        set(mutable, field, value);
        setDocument(mutable);
    }

    async function signup(email: string, password: string, firstName: string, lastName: string): Promise<firebase.auth.UserCredential> {
        const emailNoSpaces = email.replace(/ /g, "");
        log.debug("SignupWithEmail:email", emailNoSpaces, "password:", password);
        auth.setPersistence(firebase.auth.Auth.Persistence.SESSION);
        createUser(firstName, lastName);
        const verificationResult = await (auth.createUserWithEmailAndPassword(emailNoSpaces, password));
        verificationResult.user?.sendEmailVerification();
        log.debug("sendEmailVerification result:", verificationResult);
        return verificationResult;
    }

    return {changeField, saveUser, createUser, document, setDocument, signup};

}