using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.RegularExpressions;
using Bogus;
using Microsoft.Extensions.Configuration;
using Minio;
using Polly;
using Polly.Retry;
using TdoTGuide.Server.Common;

var config = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();

var projectStore = new PgsqlProjectStore(config.GetConnectionString("Pgsql") ?? throw new Exception("Can't find ConnectionStrings\\Pgsql."));
var minioClient = new MinioClient()
    .WithEndpoint(config.GetSection("Minio")["Endpoint"])
    .WithSSL(config.GetSection("Minio").GetValue("UseSSL", true))
    .WithCredentials(
        config.GetSection("Minio")["AccessKey"],
        config.GetSection("Minio")["SecretKey"]
    )
    .Build();
var projectMediaStore = new MinioProjectMediaStore(minioClient);

var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web);
var projects = JsonSerializer.Deserialize<List<ProjectData>>(File.ReadAllText("sample-projects.json"), jsonOptions) ?? throw new Exception("Can't deserialize projects.");
var organizers = (JsonSerializer.Deserialize<List<Organizer>>(File.ReadAllText("organizer-candidates.json"), jsonOptions) ?? throw new Exception("Can't deserialize organizer candidates."))
    .Select(v => new ProjectOrganizer(v.Id, v.GivenName, v.Surname, Regex.Replace(v.UserPrincipalName, "@.*$", "")))
    .ToList();

var notFoundRetryPolicy = new ResiliencePipelineBuilder()
    .AddRetry(new RetryStrategyOptions()
    {
        MaxRetryAttempts = 5,
        ShouldHandle = new PredicateBuilder().Handle<HttpRequestException>(e => e.StatusCode == HttpStatusCode.NotFound)
    })
    .Build();

Randomizer.Seed = new Random(0);
var faker = new Faker();
foreach (var projectData in projects) {
    Console.WriteLine($"Storing {projectData.Name}");
    var projectId = faker.Random.Guid().ToString();

    await projectStore.Delete(projectId);
    var existingMedia = await projectMediaStore.GetAllMediaNames(projectId).ToList();
    await projectMediaStore.RemoveMedia(projectId, existingMedia);

    var timeSelection = faker.PickRandom<ITimeSelection>(
        new ContinuousTimeSelection(),
        new RegularTimeSelection(faker.Random.Number(1, 24) * 5),
        new IndividualTimeSelection(
            Enumerable.Range(0, faker.Random.Number(1, 10))
                .Select(_ => new DateTime(DateTime.UtcNow.Date
                    .AddDays(faker.Random.Number(0, 2))
                    .AddHours(faker.Random.Number(8, 17))
                    .AddMinutes(faker.Random.Number(0, 3) * 15)
                    .Ticks, DateTimeKind.Unspecified)
                )
                .ToList()
            )
    );
    var project = new Project(
        projectId,
        projectData.Name,
        projectData.Description,
        projectData.Group,
        projectData.Departments,
        projectData.Location,
        faker.PickRandom(organizers),
        faker.Random.ListItems((IList<ProjectOrganizer>)organizers, faker.Random.Number(0, 3)),
        timeSelection);
    await projectStore.Create(project);

    using var httpClient = new HttpClient();
    var projectMediaTasks = Enumerable.Range(0, faker.Random.Number(0, 5))
        .Select(async _ => await notFoundRetryPolicy.ExecuteAsync(async ct =>
        {
            var (width, height) = faker.PickRandom((200, 300), (300, 200), (640, 480), (480, 640), (1920, 1080), (1080, 1920));
            var imageUrl = new UriBuilder(faker.Image.PicsumUrl(width, height));
            var extension = faker.PickRandom(".jpg", ".webp");
            imageUrl.Path = imageUrl.Path.TrimEnd('/') + extension;
            var content = await httpClient.GetByteArrayAsync(imageUrl.ToString(), ct);
            return new { Name = faker.Lorem.Slug() + extension, Content = content };
        }));
    var projectMedia = await Task.WhenAll(projectMediaTasks);
    var uploadUrls = await projectMediaStore.GetNewMediaUploadUrls(project.Id, projectMedia.Select(v => v.Name)).ToList();
    var uploadTasks = projectMedia.Zip(uploadUrls, async (projectMedia, uploadUrl) => {
        using var request = new HttpRequestMessage(HttpMethod.Put, uploadUrl);
        request.Content = new ByteArrayContent(projectMedia.Content);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue(Path.GetExtension(projectMedia.Name) switch
        {
            ".jpg" => "image/jpeg",
            ".png" => "image/png",
            _ => "application/octet-stream",
        });
        return await httpClient.SendAsync(request);
    });
    var uploadResults = await Task.WhenAll(uploadTasks);
    foreach (var uploadResult in uploadResults) {
        uploadResult.EnsureSuccessStatusCode();
    }
}

record ProjectData(string Name, string Group, string Location, string Description, string[] Departments);
record Organizer(string Id, string GivenName, string Surname, string UserPrincipalName);