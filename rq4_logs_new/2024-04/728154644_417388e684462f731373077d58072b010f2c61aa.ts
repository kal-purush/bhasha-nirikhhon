import gql from "graphql-tag";
import { graphQLClient } from "@/config/gqlConfig";
import { CourseType } from "./course";

export interface AssessmentType {
    id?: string;
    type: string;
    course: CourseType|string;
    uploadedDate?: number;
    deadline: string;
  fileUrl: string;
  name?: string;
  }
  
const gqlQuery = gql`
query getAllAssessments{
  getAllAssessments {
    id,
    type,
    uploadedDate,
    deadline,
    fileUrl,
    name,
    course{
      name
    }   
  }
}
`;

const createAssessmentMutation = gql`
  mutation CreateAssessment(
    $type: String!
    $course: String!
    $deadline: String!
    $fileUrl: String!
    $name:String!
  ) {
    createAssessment(
      type: $type
      course: $course
      deadline: $deadline
      fileUrl: $fileUrl
      name:$name
    ) {
      success
      code
    }
  }
`;

export const getAssessments = async():Promise<AssessmentType[]|undefined> => {
    try {
        const response:any = await graphQLClient.request(gqlQuery);
        return response.getAllAssessments;
    } catch (error) {
        console.log(`An error occurred while fetching all course. Due to this error:${error}`);
    }
}

export const AddNewAssessment = async (
  variables: AssessmentType
): Promise<{ success: boolean; message:string}> => {
  try {
    const response:any=
      await graphQLClient.request(createAssessmentMutation, {
        ...variables,
      });

    return response?.createAssessment ?? { success: false, code: "" };
  } catch (error) {
    throw error;
  }
};