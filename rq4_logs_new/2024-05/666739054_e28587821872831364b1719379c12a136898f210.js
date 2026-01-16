
export const updateLabelValue = (pinNumber, value) => ({
    type: 'UPDATE_LABEL_VALUE',
    payload: { pinNumber, value }
  });
  
  export const resetLabelValue = () => ({
    type: 'RESET_LABEL_VALUE',
  });

  export const resetLabelState = () => ({
    type: 'RESET_LABEL_STATE',
  });