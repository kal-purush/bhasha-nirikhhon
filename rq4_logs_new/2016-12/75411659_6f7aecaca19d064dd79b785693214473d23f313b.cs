using System;
using Newtonsoft.Json;

namespace Narochno.Lambda.Slack
{
    public static class Program
    {
        public static string json =
            "{\"version\":\"0\",\"id\":\"46848ad9-79bf-447c-b4ea-937ff593ea8d\",\"detail-type\":\"ECS Task State Change\",\"source\":\"aws.ecs\",\"account\":\"807103944602\",\"time\":\"2016-12-15T17:08:44Z\",\"region\":\"eu-west-1\",\"resources\":[\"arn:aws:ecs:eu-west-1:807103944602:task/a0d082ff-0cc3-4e68-9426-bf7006a7bb6f\"],\"detail\":{\"clusterArn\":\"arn:aws:ecs:eu-west-1:807103944602:cluster/Narochno_beta\",\"containerInstanceArn\":\"arn:aws:ecs:eu-west-1:807103944602:container-instance/71e0a412-b44a-49a5-bcb7-529b8816cabd\",\"containers\":[{\"containerArn\":\"arn:aws:ecs:eu-west-1:807103944602:container/4aeeb25b-351c-497a-b008-7a1cd42f5307\",\"exitCode\":0,\"lastStatus\":\"STOPPED\",\"name\":\"jobs\",\"taskArn\":\"arn:aws:ecs:eu-west-1:807103944602:task/a0d082ff-0cc3-4e68-9426-bf7006a7bb6f\"}],\"createdAt\":\"2016-12-15T17:02:57.695Z\",\"desiredStatus\":\"STOPPED\",\"lastStatus\":\"RUNNING\",\"overrides\":{\"containerOverrides\":[{\"name\":\"jobs\"}]},\"startedAt\":\"2016-12-15T17:02:59.664Z\",\"startedBy\":\"ecs-svc/9223370555039069146\",\"stoppedReason\":\"Scaling activity initiated by (deployment ecs-svc/9223370555039069146)\",\"updatedAt\":\"2016-12-15T17:08:44.088Z\",\"taskArn\":\"arn:aws:ecs:eu-west-1:807103944602:task/a0d082ff-0cc3-4e68-9426-bf7006a7bb6f\",\"taskDefinitionArn\":\"arn:aws:ecs:eu-west-1:807103944602:task-definition/Narochno_beta_jobs:6\",\"version\":5}}";
        public static void Main(string[] args)
        {
            var input = JsonConvert.DeserializeObject<CloudWatchEvent<EcsEventDetail>>(json);

            var ob2 = new
            {
                attachments = new[]
                    {
                        new
                        {
                            fallback = $"[{input.DetailType}] [{string.Join(",", input.Resources)}].",
                            color = "good",
                            title = input.DetailType,
                            text = JsonConvert.SerializeObject(input.Detail),
                        }
                    }
            };
            Console.WriteLine(ob2);
            Console.ReadLine();
        }
    }
}