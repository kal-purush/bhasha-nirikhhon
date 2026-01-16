const nav = (state = "WARMUP", action) => {
  switch (action.type) {
    case 'WARMUP':
      return "WARMUP"
    case 'WORKOUT':
      return "WORKOUT"
    case 'ENDING':
      return "ENDING"
    default:
      return state
  }
}

export default nav