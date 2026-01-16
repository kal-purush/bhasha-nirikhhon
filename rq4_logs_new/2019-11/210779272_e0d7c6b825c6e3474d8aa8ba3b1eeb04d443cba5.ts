import request from '@/utils/request';

export function getDefaultStats() {
  return {
    internships: {
      archived: 0,
      attributed: 0,
      availables: 0,
      suggested: 0,
      total: 0,
      validated: 0,
      waiting: 0,
    },
    mentors: 0,
    propositions: 0,
    students: 0,
  };
}

export function getDefaultCampaignStats() {
  return {
    campaign: 1,
    internships: {
      attributed: 0,
      availables: 0,
      total: 0,
    },
    mentors: 0,
    propositions: 0,
    students: 0,
  };
}

export const getGlobalStatistics = () =>
  request({ url: `/statistics`, method: 'get' });

export const getCampaignStatistics = (id: number) =>
  request({ url: `/campaigns/${id}/statistics`, method: 'get' });