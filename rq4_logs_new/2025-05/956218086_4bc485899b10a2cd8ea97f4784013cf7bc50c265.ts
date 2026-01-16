import { useCallback, useEffect, useRef } from 'react';
import { useReactFlow } from '@xyflow/react';
import { useDebouncedCallback } from 'use-debounce';
import { usePilotStoreShallow } from '../stores/pilot';
import { pilotEmitter } from '../events/pilot';
import { ResponseNodeMeta } from '@refly-packages/ai-workspace-common/components/canvas/nodes';

/**
 * Hook that follows pilot steps by automatically fitting view to nodes associated
 * with active pilot session steps
 *
 * - Registers a singleton listener for the pilotEmitter
 * - When pilotStepCreate event is received, finds nodes with matching pilotSessionId
 * - Fits view to those nodes if they have status 'waiting' or 'executing'
 */
export const useFollowPilotSteps = () => {
  // Keep track of whether listener has been registered
  const registeredRef = useRef(false);

  // Get React Flow instance to control the canvas view
  const reactFlowInstance = useReactFlow();

  // Get active session ID from pilot store
  const { activeSessionId } = usePilotStoreShallow((state) => ({
    activeSessionId: state.activeSessionId,
  }));

  // Create debounced callback for fitting view to relevant nodes
  const debouncedFitView = useDebouncedCallback(() => {
    if (!activeSessionId) return;

    // Get all nodes from the React Flow instance
    const nodes = reactFlowInstance.getNodes();

    // Filter nodes matching our criteria:
    // 1. Type is skillResponse
    // 2. Has pilotSessionId matching activeSessionId
    // 3. Status is either 'waiting' or 'executing'
    const matchedNodes = nodes.filter((node) => {
      if (node.type !== 'skillResponse') return false;
      const metadata = node.data?.metadata as ResponseNodeMeta;
      return (
        metadata?.pilotSessionId === activeSessionId &&
        (metadata?.status === 'waiting' || metadata?.status === 'executing')
      );
    });

    // If we found matching nodes, fit the view to them
    if (matchedNodes.length > 0) {
      reactFlowInstance.fitView({
        padding: 0.2,
        duration: 200,
        minZoom: 0.1,
        maxZoom: 1,
        nodes: matchedNodes,
      });
    }
  }, 300); // 300ms debounce time

  // Handler for pilotStepCreate event
  const handlePilotStepCreate = useCallback(() => {
    debouncedFitView();
  }, [debouncedFitView]);

  // Set up event listener
  useEffect(() => {
    // Only register once to create a singleton listener
    if (!registeredRef.current) {
      pilotEmitter.on('pilotStepCreate', handlePilotStepCreate);
      registeredRef.current = true;
    }

    // Cleanup function
    return () => {
      // We're not removing the listener in the cleanup
      // since we want it to persist as a singleton
      // If we ever need to remove it, we would do:
      // pilotEmitter.off('pilotStepCreate', handlePilotStepCreate);
    };
  }, [handlePilotStepCreate]);

  // Return an empty object since this hook is only for side effects
  return {};
};