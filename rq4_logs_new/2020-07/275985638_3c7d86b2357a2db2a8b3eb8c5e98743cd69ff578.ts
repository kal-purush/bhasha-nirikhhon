import { GithubInvalidReason } from "~/src/common/types/common/setting-value-validation"
import { GithubSettingValue } from "~/src/common/types/common/setting-value"
import { ALL_EMPTY, OWNER_NAME_EMPTY, REPO_NAME_EMPTY } from "../../validation-object/github"
import { getInvalidReason } from "../utility"

export function validateAllNotEmpty(value:GithubSettingValue):GithubInvalidReason|undefined {
  if(validateRepoNameNotEmpty(value) != null && validateOwnerNameNotEmpty(value) != null) {
    return ALL_EMPTY()
  }
}

export function validateRepoNameNotEmpty(value:GithubSettingValue):GithubInvalidReason|undefined {
  if(value.repo.trim() == "") {
    return REPO_NAME_EMPTY()
  }
}

export function validateOwnerNameNotEmpty(value:GithubSettingValue):GithubInvalidReason|undefined {
  const owner_name = value.is_mine ? value.user_id : value.owner
  if(owner_name.trim() == "") {
    return OWNER_NAME_EMPTY()
  }
}

export function validate(new_value:GithubSettingValue):GithubInvalidReason|undefined {
  let invalid_reason = getInvalidReason<GithubInvalidReason, GithubSettingValue>([validateAllNotEmpty, validateRepoNameNotEmpty, validateOwnerNameNotEmpty], new_value)
  return invalid_reason
}