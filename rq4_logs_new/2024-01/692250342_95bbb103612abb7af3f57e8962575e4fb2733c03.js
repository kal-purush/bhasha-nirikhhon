import { gql } from '@apollo/client';

const NEW_MESSAGE_SUBSCRIPTION = gql`
    subscription newMessage($wsToken: String!) {
        newMessage(wsToken: $wsToken) {
            id
            content
            sender {
                id
                firstName
                lastName
            }
            receiver {
                id
                firstName
            }
            project {
                id
                title
            }
            phase {
                id
                title
            }
            index
            createdAt
            read
        }
    }
`;

export { NEW_MESSAGE_SUBSCRIPTION };