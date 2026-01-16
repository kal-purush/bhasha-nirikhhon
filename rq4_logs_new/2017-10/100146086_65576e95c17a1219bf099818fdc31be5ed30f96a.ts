import { Maybe } from 'monet'
import { div, form, h1, input, label, p } from 'mostly-dom'

import State from '../state'

import header from './logo'
import spinner from './spinner'

interface Props {
  partyCode: Maybe<string>
  isJoining: boolean
  error: Maybe<string>
}

export default function render(props: Props) {
  const isSubmitHiding = props.isJoining || props.partyCode.cata(
    () => true,
    partyCode => partyCode.length === 0,
  )

  return form({
    attrs: { id: 'join' },
    on: { submit: onJoinSubmitted },
  }, [
    h1('Join a Party'),
    p('Help mix things up and join the fun!'),
    div({ class: { 'form-group': true } }, [
      label({ attrs: { for: 'partyCode' } }, 'Party code:'),
      div({ class: { 'pill-group': true } }, [
        input({
          attrs: {
            id: 'partyCode',
            type: 'text',
            value: props.partyCode.orJust(''),
            placeholder: 'Ab7j',
            autofocus: true,
            autocomplete: 'off',
            maxLength: 4,
            disabled: props.isJoining,
          },
          on: {
            input: partyCodeInputChange,
            focus: focusBlurJoinForm,
            blur: focusBlurJoinForm,
          },
        }),
        input({
          class: { hiding: isSubmitHiding },
          attrs: {
            type: 'submit',
            value: 'Join',
            title: 'Join the party',
          },
          on: {
            focus: focusBlurJoinForm,
            blur: focusBlurJoinForm,
          },
        }),
      ]),
      spinner(!props.isJoining),
    ]),
    props.error.isJust() ? p({ class: { error: true } }, props.error.just()) : null,
  ])
}

let partyCode = ''
let submitVisible = false
function partyCodeInputChange(e: Event) {
  const input = e.currentTarget as HTMLInputElement
  const value = stripNonAlphaNumeric(
    input.value.trim().replace(whitespace, ''),
  )
  input.value = value
  partyCode = value

  submitVisible = value.length > 0
}

const whitespace = /[\s\\\^\[\]`]/g
const alphaNum = /[a-zA-z0-9]/
function stripNonAlphaNumeric(s: string) {
  let result = ''
  for (let char = 0; char < s.length; char++) {
    const c = s.charAt(char)
    result += alphaNum.test(c) ? c : ''
  }
  return result
}

function focusBlurJoinForm(e: Event) {
  const input = e.currentTarget as HTMLInputElement
  const pillGroup = input.parentElement as HTMLElement

  if (e.type === 'focus') {
    pillGroup.classList.add('focus')
  } else {
    pillGroup.classList.remove('focus')
  }
}

function onJoinSubmitted(e: Event) {
  e.preventDefault()

  if (partyCode && partyCode.trim()) {
    // TODO: Blur the form
    // this.partyCode.blur()
    // this.submitButton.blur()

    State.joinParty(partyCode)
  }
}