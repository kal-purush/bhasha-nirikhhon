import { useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useFrontPageStoreShallow } from '../stores/front-page';
import { genActionResultID } from '@refly/utils/id';
import { useInvokeAction } from '@refly-packages/ai-workspace-common/hooks/canvas/use-invoke-action';
import { useAddNode } from '@refly-packages/ai-workspace-common/hooks/canvas/use-add-node';
import { useChatStoreShallow } from '@refly-packages/ai-workspace-common/stores/chat';

export const useCanvasInitialActions = (canvasId: string) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { addNode } = useAddNode();
  const { invokeAction } = useInvokeAction();
  const { query, selectedSkill, runtimeConfig, tplConfig, reset } = useFrontPageStoreShallow(
    (state) => ({
      query: state.query,
      selectedSkill: state.selectedSkill,
      runtimeConfig: state.runtimeConfig,
      reset: state.reset,
    }),
  );

  const { skillSelectedModel } = useChatStoreShallow((state) => ({
    skillSelectedModel: state.skillSelectedModel,
  }));

  useEffect(() => {
    const source = searchParams.get('source');
    const newParams = new URLSearchParams();

    // Copy all params except 'source'
    for (const [key, value] of searchParams.entries()) {
      if (key !== 'source') {
        newParams.append(key, value);
      }
    }
    setSearchParams(newParams);

    // Only proceed if source is 'front-page' and we have a query
    if (source === 'front-page' && query?.trim() && canvasId) {
      console.log('Front page initial action:', {
        canvasId,
        query,
        selectedSkill,
      });

      const resultId = genActionResultID();
      invokeAction(
        {
          query,
          resultId,
          selectedSkill,
          modelInfo: skillSelectedModel,
          tplConfig,
          runtimeConfig,
        },
        {
          entityId: canvasId,
          entityType: 'canvas',
        },
      );
      addNode({
        type: 'skillResponse',
        data: {
          title: query,
          entityId: resultId,
          metadata: {
            status: 'executing',
            selectedSkill,
            modelInfo: skillSelectedModel,
            runtimeConfig,
            tplConfig,
            structuredData: {
              query,
            },
          },
        },
      });

      reset();
    }
  }, [canvasId, query, selectedSkill, searchParams, reset]);
};