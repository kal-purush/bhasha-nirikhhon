/**
 * Header Editor styled-components.
 * For documentation for styled-components, see:
 * https://www.styled-components.com/
 */

// Vendor modules
import { Tag } from "@blueprintjs/core";
import styled from "styled-components";

// Styled components
export const EditableTag = styled(Tag)`
  background: "#f1f8ff";
  color: "#0366d6";
  margin-right: "5px";
`;

export const AuthorTag = styled(Tag)`
  background: "#E5E5E5";
  font-style: "italic";
  margin-right: "5px";
`;

export const EditableAuthorTag = styled(AuthorTag)`
  color: "black";
`;

export const Container = styled.div`
  background: "#EEE";
  padding: "10px";
`;

export const MarginContainer = styled.div`
  margin-top: "10px";
`;