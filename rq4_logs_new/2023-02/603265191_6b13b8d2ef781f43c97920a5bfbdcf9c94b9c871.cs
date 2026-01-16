namespace AzurePipelines.Toolkit;

public static class Constants
{
    public static string? AgentBuildDirectory => EnvironmentInfo.GetVariable("AGENT_BUILDDIRECTORY");

    public static string? AgentContainerMapping => EnvironmentInfo.GetVariable("AGENT_CONTAINERMAPPING");

    public static string? AgentHomeDirectory => EnvironmentInfo.GetVariable("AGENT_HOMEDIRECTORY");

    public static string? AgentId => EnvironmentInfo.GetVariable("AGENT_ID");

    public static string? AgentJobName => EnvironmentInfo.GetVariable("AGENT_JOBNAME");

    public static string? AgentJobStatus => EnvironmentInfo.GetVariable("AGENT_JOBSTATUS");
}