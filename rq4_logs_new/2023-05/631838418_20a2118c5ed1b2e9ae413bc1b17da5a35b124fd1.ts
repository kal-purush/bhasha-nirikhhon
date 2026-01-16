import { useEffect, useMemo, useState } from 'react';
import axios from 'axios';

import { SERVER_URL } from 'src/constants';

const DELAY_SEC = 5000;

function useInferenceCheck(start: boolean, id: number) {
  const [status, setStatus] = useState(-1);
  const isInferenceFinished = useMemo(() => status === 2, [status]);

  const callback = async () => {
    const { status: newStatus } = (
      await axios(SERVER_URL + '/api/memberImages/inference_check', { params: { id } })
    ).data;
    setStatus(newStatus);
  };

  useEffect(() => {
    if (start) {
      const interval = setInterval(callback, DELAY_SEC);

      if (status !== 1) {
        if (status === 0) {
          console.log('fail');
        }
        return () => {
          clearInterval(interval);
        };
      }
    }
  }, [start, status]);

  return isInferenceFinished;
}

export default useInferenceCheck;