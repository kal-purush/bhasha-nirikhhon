export const SAMPLE_ACTION  = 'SAMPLE_ACTION';

export const SAMPLE_ASYNC   = 'SAMPLE_ASYNC';
export const SAMPLE_SUCCESS = 'SAMPLE_SUCCESS';
export const SAMPLE_FAILURE = 'SAMPLE_FAILURE';

export function changeAsyncSampleText(text) {
  return (dispatch, getState) => {

    dispatch(changeSampleText('LOADING...'));

    let p = new Promise((resolve, reject) => {

      let b = Math.random() > 0.5 ? true : false;

      setTimeout(() => {
        if (b === true) {
          resolve('Success!');
        } else {
          reject('Failure!');
        }
      }, 1000);

    });

    return p.then(s => dispatch(changeSampleText(s)), e => dispatch(changeSampleText(e)));
  };
}

export function changeSampleText(text) {
  return {type: SAMPLE_ACTION, text};
}