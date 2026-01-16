using System.Collections.Generic;
using System.Threading.Tasks;
using Pulumi;
using Pulumi.AzureNextGen.Resources.Latest;
using Pulumi.AzureNextGen.Storage.Latest;
using Pulumi.AzureNextGen.Storage.Latest.Inputs;
using Pulumi.AzureNextGen.Web.Latest;
using Pulumi.AzureNextGen.Web.Latest.Inputs;

class ToDoApiStack : Stack
{
    public ToDoApiStack()
    {
        var config = new Config();
        var environment = config.Require("env");
        string appName = "todoapi";
        string resourceGroupName = $"{ResourcePrefixes.ResourceGroup}{appName}-{environment}";
        string funcStorageName = $"{ResourcePrefixes.Storage}{appName}{environment}001";
        string funcAppServicePlanName = $"{ResourcePrefixes.AppServicePlan}{appName}-{environment}";
        string funcAppName = $"{ResourcePrefixes.FunctionApp}{appName}-{environment}";

        //1. Create Resource Group
        var resourceGroup = new ResourceGroup(resourceGroupName, new ResourceGroupArgs
        {
            ResourceGroupName = resourceGroupName,
            Location = "southeast asia"
        });

        //2. Create Storage Account for Func App
        var storageAccount = new StorageAccount(funcStorageName, new StorageAccountArgs
        {
            ResourceGroupName = resourceGroup.Name,
            AccountName = funcStorageName,
            Location = resourceGroup.Location,
            Sku = new SkuArgs
            {
                Name = SkuName.Standard_LRS
            },
            Kind = Kind.StorageV2
        });

        // Export the primary key of the Storage Account
        this.PrimaryStorageKey = Output.Tuple(resourceGroup.Name, storageAccount.Name).Apply(names =>
            Output.CreateSecret(GetStorageAccountPrimaryKey(names.Item1, names.Item2)));

        //3. Create Consumption Plan for Func App
        var appServicePlan = new AppServicePlan(funcAppServicePlanName, new AppServicePlanArgs()
        {
            Name = funcAppServicePlanName,
            ResourceGroupName = resourceGroup.Name,
            Location = resourceGroup.Location,
            Kind = "FunctionApp",
            Sku = new SkuDescriptionArgs
            {
                Name = "Y1",
                Tier = "Dynamic",
                Size = "Y1",
            },
        });            

        //4. Create Function App
        var functionApp = new WebApp(funcAppName, new WebAppArgs()
        {
            Name = funcAppName,
            ResourceGroupName = resourceGroup.Name,
            Location = resourceGroup.Location,
            ServerFarmId = appServicePlan.Id,
            Kind = "functionapp",
            SiteConfig = new SiteConfigArgs()
            {
                //	5.1 Set up Configuration
                AppSettings = new List<NameValuePairArgs>()
                {
                    new NameValuePairArgs
                    { 
                        Name = "AzureWebJobsStorage", 
                        Value = Output.Format($"DefaultEndpointsProtocol=https;AccountName={storageAccount.Name};AccountKey={this.PrimaryStorageKey};EndpointSuffix=core.windows.net;")
                    },
                    new NameValuePairArgs
                    { 
                        Name = "AzureWebJobsDashboard", 
                        Value = Output.Format($"DefaultEndpointsProtocol=https;AccountName={storageAccount.Name};AccountKey={this.PrimaryStorageKey};EndpointSuffix=core.windows.net;")
                    },
                    new NameValuePairArgs{ Name = "Runtime", Value = "dotnet"},
                    new NameValuePairArgs{ Name = "FUNCTIONS_EXTENSION_VERSION", Value = "~3"}
                },
            },
        });
    }

    [Output]
    public Output<string> PrimaryStorageKey { get; set; }

    private static async Task<string> GetStorageAccountPrimaryKey(string resourceGroupName, string accountName)
    {
        var accountKeys = await ListStorageAccountKeys.InvokeAsync(new ListStorageAccountKeysArgs
        {
            ResourceGroupName = resourceGroupName,
            AccountName = accountName
        });
        return accountKeys.Keys[0].Value;
    }
}