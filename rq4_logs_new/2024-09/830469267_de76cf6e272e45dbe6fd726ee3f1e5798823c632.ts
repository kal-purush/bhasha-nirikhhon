import { gql } from "@apollo/client";

export const PROJECT_UPDATES_SUBSCRIPTION = gql`
    subscription ($projectIds: [Int!]!) {
        projectUpdate(projectIds: $projectIds) {
            message
            type
        }
    }
`;

export const BID_UPDATES_SUBSCRIPTION = gql`
    subscription ($bidIds: [Int!]!) {
        bidUpdate(bidIds: $bidIds) {
            message
            type
        }
    }
`;

export const MESSAGE_SUBSCRIPTION_QUERY = gql`
    subscription ($projectId: Int!) {
        messageCreated(projectId: $projectId) {
            id
            text
            projectId
            userId
        }
    }
`;

export const MESSAGES_QUERY = gql`
    query Messages($projectId: Int!) {
        messages(projectId: $projectId) {
            id
            text
            projectId
            userId
        }
    }
`;

export const POST_MESSAGE_QUERY = gql`
    mutation ($message: MessageInput!) {
        postMessage(message: $message) {
            id
            text
            projectId
            userId
        }
    }
`;