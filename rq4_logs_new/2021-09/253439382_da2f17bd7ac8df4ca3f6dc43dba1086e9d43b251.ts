import WorkflowService from "@/services/WorkflowService";
import axios from "axios";

describe("WorkflowService.ts", () => {
  beforeEach(() => {
    jest.resetAllMocks();
    axios.get = jest.fn().mockResolvedValue("axios get return");
  });

  it("can getWorkflows", async () => {
    const api = process.env.VUE_APP_API;
    const result = await WorkflowService.getWorkflows();
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "workflow");
    expect(result).toBe("axios get return");
  });

  it("can getWorkflowTasks", async () => {
    const api = process.env.VUE_APP_API;
    const result = await WorkflowService.getWorkflowTasks();
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "workflow/tasks");
    expect(result).toBe("axios get return");
  });
});