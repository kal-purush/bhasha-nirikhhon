const nameMaxLength = 20
const nameMinLength = 4
const regex = /^[a-zA-Z0-9_.]+$/

function channelNameValidator(name) {
  const isValidMaxLength = name.length <= nameMaxLength
  const isValidMinLength = name.length >= nameMinLength
  const isValidPattern = regex.test(name)

  if (!isValidMaxLength) {
    throw new Error(`Name must be less than ${nameMaxLength} characters`)
  }

  if (!isValidMinLength) {
    throw new Error(`Name must be more than ${nameMinLength} characters`)
  }

  if (!isValidPattern) {
    throw new Error(`Name must contain valid characters`)
  }
}

export default channelNameValidator