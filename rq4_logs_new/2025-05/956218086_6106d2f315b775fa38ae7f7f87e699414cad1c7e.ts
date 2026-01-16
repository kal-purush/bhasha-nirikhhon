import { useCallback } from 'react';
import { usePilotStoreShallow } from '@refly-packages/ai-workspace-common/stores/pilot';
import getClient from '@refly-packages/ai-workspace-common/requests/proxiedRequest';
import { CreatePilotSessionRequest } from '@refly/openapi-schema';

export const usePilotManagement = () => {
  const { activeSession } = usePilotStoreShallow((state) => ({
    activeSession: state.activeSession,
  }));

  const createPilotSession = useCallback(async (param: CreatePilotSessionRequest) => {
    const { data, error } = await getClient().createPilotSession({
      body: param,
    });

    if (error) {
      return null;
    }

    return data?.data?.sessionId;
  }, []);

  return {
    activeSession,
    createPilotSession,
  };
};