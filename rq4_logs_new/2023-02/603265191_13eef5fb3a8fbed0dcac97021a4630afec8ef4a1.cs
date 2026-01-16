namespace AzurePipelines.Toolkit;

public static class Variables
{
    public static class Agent
    {
        public static string? BuildDirectory => EnvironmentInfo.GetVariable("AGENT_BUILDDIRECTORY");

        public static string? ContainerMapping => EnvironmentInfo.GetVariable("AGENT_CONTAINERMAPPING");

        public static string? HomeDirectory => EnvironmentInfo.GetVariable("AGENT_HOMEDIRECTORY");

        public static string? Id => EnvironmentInfo.GetVariable("AGENT_ID");

        public static string? JobName => EnvironmentInfo.GetVariable("AGENT_JOBNAME");

        public static string? JobStatus => EnvironmentInfo.GetVariable("AGENT_JOBSTATUS");

        public static string? MachineName => EnvironmentInfo.GetVariable("AGENT_MACHINENAME");

        public static string? Name => EnvironmentInfo.GetVariable("AGENT_NAME");

        public static string? OS => EnvironmentInfo.GetVariable("AGENT_OS");

        public static string? OSArchitecture => EnvironmentInfo.GetVariable("AGENT_OSARCHITECTURE");

        public static string? TempDirectory => EnvironmentInfo.GetVariable("AGENT_TEMPDIRECTORY");

        public static string? ToolsDirectory => EnvironmentInfo.GetVariable("AGENT_TOOLSDIRECTORY");

        public static string? WorkFolder => EnvironmentInfo.GetVariable("AGENT_WORKFOLDER");
    }

    public static class Build
    {
    }

    public static class Pipeline
    {
    }

    public static class Deployment
    {
    }

    public static class System
    {
    }

    public static class Checks
    {
    }
}