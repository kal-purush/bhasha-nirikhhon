import { useCallback } from "react";
import { useAgentStore } from "@/store/agentStore";
import { AgentStatus } from "../types";
import { showToast } from "@/lib/toast";

export function useAgentControls(id: string, status: AgentStatus) {
  const { updateAgentStatus, restartAgent } = useAgentStore((state) => ({
    updateAgentStatus: state.updateAgentStatus,
    restartAgent: state.restartAgent,
  }));

  const handleStatusChange = useCallback(async () => {
    try {
      const newStatus = status === "running" ? "paused" : "running";
      await updateAgentStatus(id, newStatus);
      showToast.success(
        `Agent ${status === "running" ? "paused" : "started"} successfully`,
      );
    } catch (error) {
      showToast.error(
        error.message ||
          `Failed to ${status === "running" ? "pause" : "start"} agent`,
      );
    }
  }, [id, status, updateAgentStatus]);

  const handleRestart = useCallback(async () => {
    try {
      await restartAgent(id);
      showToast.success("Agent restarted successfully");
    } catch (error) {
      showToast.error(error.message || "Failed to restart agent");
    }
  }, [id, restartAgent]);

  return { handleStatusChange, handleRestart };
}