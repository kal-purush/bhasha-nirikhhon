import { gql } from "@apollo/client"

export const CREATETAG = gql`
 mutation createTag($name: String!) {
    createTag(name: $name) {
        id
      name
    }
  }`