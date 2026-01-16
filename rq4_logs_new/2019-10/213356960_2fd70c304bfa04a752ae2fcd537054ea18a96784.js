const prefix = 'hvr-'
const hoverAnimations = [
  'grow',
  'shrink',
  'pulse',
  'pulse-grow',
  'pulse-shrink',
  'push',
  'pop',
  'bounce-in',
  'bounce-out',
  'rotate',
  'grow-rotate',
  'float',
  'sink',
  'bob',
  'hang',
  'skew-forward',
  'skew-backward',
  'wobble-horizontal',
  'wobble-vertical',
  'wobble-skew',
  'wobble-to-bottom-right',
  'wobble-to-top-right',
  'buzz',
  'buzz-out',
  'forward',
  'backward'
]

export function getrandomAnimation () {
  const index = Math.floor(Math.random() * hoverAnimations.length)
  const animation = hoverAnimations[index]
  return `${prefix}${animation}`
}