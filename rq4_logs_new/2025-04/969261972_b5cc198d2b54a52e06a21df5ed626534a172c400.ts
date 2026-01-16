import { AssignmentResponse } from "@/app/types/assignmentResponse ";

export type AssignmentTypeValue = "" | "solo" | "group" | "review";

// Determine if assignment is a solo assignment
export const isSoloAssignment = (assignment: AssignmentResponse): boolean => {
  return !assignment.groupId && !assignment.previousAssignmentId;
};

// Determine if assignment is a group assignment
export const isGroupAssignment = (assignment: AssignmentResponse): boolean => {
  return !!assignment.groupId;
};

// Determine if assignment is a review assignment
export const isReviewAssignment = (assignment: AssignmentResponse): boolean => {
  return !!assignment.previousAssignmentId;
};

// Get the assignment type value
export const getAssignmentType = (assignment?: AssignmentResponse): AssignmentTypeValue => {
  if (!assignment) return "";
  if (isReviewAssignment(assignment)) return "review";
  if (isGroupAssignment(assignment)) return "group";
  return "solo";
};