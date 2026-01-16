import { useAxios } from '../use-axios';
import { useSnackbar } from '../use-snackbar';

export const useProposal = () => {
  const { post, get } = useAxios();
  const { open } = useSnackbar();
  const filterType = {
    pageNo: 1,
    pageSize: 10,
    title: '',
    category: ''
  };

  const saveProposal = async proposalData => {
    {
      proposalData.Amount = parseInt(proposalData.Amount);
      return await post('/proposal', proposalData);
    }
  };

  // const getAll = async filter => {
  //   {
  //     try {
  //       const allJobs = await get(`/job?filter=${JSON.stringify(filter)}`);
  //       return allJobs;
  //     } catch {}
  //   }
  // };

  // const getById = async id => {
  //   {
  //     try {
  //       const job = await get(`/job/get-by-id/${id}`);
  //       return job;
  //     } catch {}
  //   }
  // };

  return {
    saveProposal
  };
};