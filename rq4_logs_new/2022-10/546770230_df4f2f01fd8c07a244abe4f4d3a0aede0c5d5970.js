import api from "./api.js";

async function createAsset(data, token) {
  const config = {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  };

  const response = await api.post(`/assets`, data, config);
  return response.data;
}

async function getAsset(assetId, token) {
  const config = {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  };

  const response = await api.get(`/assets/${assetId}`, config);
  return response.data;
}

async function getHealth(assetId, token) {
  const config = {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  };

  const response = await api.get(`/asset/health/${assetId}`, config);
  return response.data;
}
async function getStatus(assetId, token) {
  const config = {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  };

  const response = await api.get(`/asset/status/${assetId}`, config);
  return response.data;
}

async function updateHealth(data, token) {
  const config = {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  };

  const response = await api.post(`/asset/health/`, data, config);
  return response.data;
}

async function updateStatus(data, token) {
  const config = {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  };

  const response = await api.post(`/asset/status/`, data, config);
  return response.data;
}

const assetsService = {
  createAsset,
  getAsset,
  getHealth,
  updateHealth,
  updateStatus,
  getStatus,
};

export default assetsService;