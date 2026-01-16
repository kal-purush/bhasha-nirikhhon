import { TeacherType } from "./teacher";
import { StudentType } from "./student";
import gql from "graphql-tag";
import { graphQLClient } from "@/config/gqlConfig";

const getConversationQuery = `
  query GetConversation($receiverId: String!, $senderId: String!) {
    getConversation(receiverId: $receiverId, senderId: $senderId) {
      id
      participants {
        firstName
        id
        lastName
      }
      messages {
        message
        id
        senderId
        receiverId
      }
    }
  }
`;
const createMessageMutation = `
  mutation CreateMessage($senderId: String!, $receiverId: String!, $message: String!) {
    createMessage(senderId: $senderId, receiverId: $receiverId, message: $message) {
      success
      message
      data
    }
  }
`;

const variables = {
  senderId: "661baf229d2e0fa0b53f3099",
  receiverId: "661bae479d2e0fa0b53f3094",
  message: "How are you?"
};


export const getConversationByParticipants = async (participants:any): Promise<any> => {
    try {      
       const response: any = await graphQLClient.request(getConversationQuery, participants);

        return response.getConversation;
    } catch (error) {
        console.log(`An error occurred while fetching all course. Due to this error:${error}`);
    }
}

export const SendMessageApi = async (
  variables: any
): Promise<any> => {
  try {
    const response:any=
      await graphQLClient.request(createMessageMutation, {
        ...variables,
      });

      console.log("================:",response)
    return response?.createMessage ?? { success: false, messsage:"",data:null };
  } catch (error) {
    throw error;
  }
};