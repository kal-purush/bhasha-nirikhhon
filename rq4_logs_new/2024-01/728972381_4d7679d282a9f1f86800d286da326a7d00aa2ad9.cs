using Newtonsoft.Json;
using System.Diagnostics;
using System.Text;
using VpnetworkAPI.Models;


namespace BackgroundServiceWorker
{
    public class BackgroundServices : BackgroundService
    {
        private readonly ILogger<BackgroundServices> _logger;
        private Timer timer;
        private string apiUrl = "https://localhost:7177/";
        private string userId = "String"; // Replace with dynamic user
        private List<ProgramData> allProgramData;
        private List<Analysis> analysisData;

        private const double DefaultLocalMemoryThreshold = 100000; // Replace with your default value
        private const double DefaultLocalNetworkThreshold = 10000; // Replace with your default value

        // Define memory and network speed threshold

        public BackgroundServices(ILogger<BackgroundServices> logger)
        {
            _logger = logger;
            allProgramData = new List<ProgramData>();
            analysisData = new List<Analysis>();
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            timer = new Timer(TimerElapsed, null, 0, 60000); // Run every 60 seconds 
            return base.StartAsync(cancellationToken);
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            timer?.Change(Timeout.Infinite, 0);
            return base.StopAsync(cancellationToken);
        }

        private async void TimerElapsed(object state)
        {
            try  
            {
                Process[] processes = Process.GetProcesses();

                foreach (Process process in processes)
                {
                    try
                    {
                        double MemoryThreshold = 0.0;
                        double NetworkThreshold = 0.0;
                        string processName = process.ProcessName;
                        int pid = process.Id;
                        long memoryUsage = process.WorkingSet64 / 1024;
                        double networkSpeed = GetNetworkSpeed(processName);

                        ThresholdSettings thresholdSetting = GetThresholdTypeSettings(userId, processName);

                        if (thresholdSetting != null)
                        {
                            Console.WriteLine("We are at the thresholdSetting");
                            switch (thresholdSetting.ThresholdSetting.ToLower())
                            {
                                case "local":
                                    // Check for local settings
                                    LocalProgramData localSettings = GetLocalThresholdSettings(userId, processName);

                                    if (localSettings == null)
                                    {
                                        // Prompt user to add local setting if not present
                                        _logger.LogWarning($"Local threshold setting not found for {processName}. Please add the setting manually.");
                                    }
                                    else
                                    {

                                        MemoryThreshold = localSettings.ProgramLocalMemoryThreshold;
                                        NetworkThreshold = localSettings.ProgramLocalNetworkThreshold;
                                    }
                                    // Switch to local settings
                                    break;

                                case "global":
                                    // Check for global settings
                                    GlobalProgramData globalDataSetting = GetGlobalThresholdSettings(processName);

                                    if (globalDataSetting == null)
                                    {
                                        AddGlobalThresholdSetting(processName);
                                        globalDataSetting = GetGlobalThresholdSettings(processName);
                                    }

                                    MemoryThreshold = globalDataSetting.ProgramGLobalMemoryThreshold;
                                    NetworkThreshold = globalDataSetting.ProgramGlobalNetworkThreshold;
                                    break;

                                default:
                                    // If no threshold setting found, use default values
                                    MemoryThreshold = DefaultLocalMemoryThreshold;
                                    NetworkThreshold = DefaultLocalNetworkThreshold;
                                    break;
                            }
                        }

                        if (memoryUsage > MemoryThreshold || networkSpeed > NetworkThreshold)
                        {
                            ProgramData existingProgram = allProgramData.Find(p => p.ProgramName == processName);
                            if (existingProgram != null)
                            {
                                existingProgram.ProgramBadCount++;
                                existingProgram.MemoryUsage += memoryUsage;
                                existingProgram.NetworkUsage += networkSpeed;
                                Console.WriteLine($"oiee this fellow {processName} will be added");
                            }
                            else
                            {
                                Console.WriteLine($"oiee this is {processName} the new fellow");
                                ProgramData programData = new ProgramData
                                {
                                    ProgramName = processName,
                                    PID = pid,
                                    MemoryUsage = memoryUsage,
                                    NetworkUsage = networkSpeed,
                                    ProgramBadCount = 1
                                };

                                allProgramData.Add(programData);
                            }
                            var analysis = new Analysis
                            {
                                UserId = userId,
                                DateTime = DateTime.Now,
                                ProgramName = processName,
                                MemoryUsage = memoryUsage,
                                NetworkUsage = networkSpeed
                            };
                            // Add the analysis data to the list
                            analysisData.Add(analysis);

                            // Send the analysis to the API
                            SendAnalysisToApi(analysis);

                        }
                    }
                    catch (Exception exception)
                    {
                        _logger.LogError($"Error retrieving information for {process.ProcessName}: {exception.Message}");
                    }
                }

                User existingUser = GetExistingUserFromApi(userId);

                //var existingProgramData = GetProgramDataFromApi(userId);

                List<ProgramData> existingProgramData = GetProgramDataFromApi(userId);

                if (existingProgramData != null)
                {
                    Console.WriteLine("Hey we are checking existing user here");

                    foreach (ProgramData programData in allProgramData)
                    {
                        ProgramData existingProgram = existingProgramData.FirstOrDefault(p => p.ProgramName == programData.ProgramName);

                        if (existingProgram != null)
                        {
                            Console.WriteLine("Hey we are checking existing program here");
                            existingProgram.MemoryUsage = programData.MemoryUsage;
                            existingProgram.ProgramBadCount += programData.ProgramBadCount;
                        }
                        else
                        {
                            Console.WriteLine($"Hey we are Add {programData.ProgramName} existing user here");
                            existingProgramData.Add(programData);
                        }
                    }
                    Console.WriteLine("Hey this is existing p data" + existingProgramData);


                    allProgramData.Clear();
                    allProgramData = existingProgramData;
                    UpdateProgramDataToApi(userId, existingProgramData);
                }
                else
                {
                    User newUser = new User
                    {
                        UserId = userId,
                        Programs = allProgramData,
                        UserSettings = null
                    };

                    SendDataToApi(newUser);
                }
                foreach (var analysis in analysisData)
                {
                    SendAnalysisToApi(analysis);
                }
                analysisData.Clear();
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error: {ex.Message}");
            }
        }


        private void UpdateProgramDataToApi(string userId, List<ProgramData> programDataList)
        {
            try
            {
                using (HttpClient httpClient = new HttpClient())
                {
                    httpClient.BaseAddress = new Uri(apiUrl);

                    string programDataJson = System.Text.Json.JsonSerializer.Serialize(programDataList);
                    _logger.LogInformation($"Payload sent to API: {programDataJson}");

                    HttpResponseMessage putResponse = httpClient.PutAsync($"users/{userId}/programs", new StringContent(programDataJson, Encoding.UTF8, "application/json")).Result;

                    if (putResponse.IsSuccessStatusCode)
                    {
                        _logger.LogInformation($"Program data updated successfully for UserId: {userId}");
                    }
                    else
                    {
                        _logger.LogError($"Failed to update program data. Status Code: {putResponse.StatusCode}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error sending data to API: {ex.Message}");
            }
        }
        private void AddGlobalThresholdSetting(string programName)
        {
            try
            {
                using (HttpClient httpClient = new HttpClient())
                {
                    httpClient.BaseAddress = new Uri(apiUrl);

                    // You can set default values or retrieve them from somewhere
                    double defaultGlobalMemoryThreshold = 50000; // Replace with your default value
                    double defaultGlobalNetworkThreshold = 5000; // Replace with your default value

                    GlobalProgramData newGlobalSetting = new GlobalProgramData
                    {
                        ProgramName = programName,
                        ProgramGLobalMemoryThreshold = defaultGlobalMemoryThreshold,
                        ProgramGlobalNetworkThreshold = defaultGlobalNetworkThreshold
                    };

                    string globalSettingJson = System.Text.Json.JsonSerializer.Serialize(newGlobalSetting);

                    HttpResponseMessage postResponse = httpClient.PostAsync("GlobalPrograms/programs", new StringContent(globalSettingJson, Encoding.UTF8, "application/json")).Result;

                    if (postResponse.IsSuccessStatusCode)
                    {
                        _logger.LogInformation($"Global threshold setting added successfully for program: {programName}");
                    }
                    else
                    {
                        _logger.LogError($"Failed to add global threshold setting for program {programName}. Status Code: {postResponse.StatusCode}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error adding global threshold setting: {ex.Message}");
            }
        }

        private List<ProgramData> GetProgramDataFromApi(string userId)
        {
            try
            {
                using (HttpClient httpClient = new HttpClient())
                {
                    httpClient.BaseAddress = new Uri(apiUrl);
                    HttpResponseMessage response = httpClient.GetAsync($"users/{userId}/programs").Result;

                    if (response.IsSuccessStatusCode)
                    {
                        string programDataJson = response.Content.ReadAsStringAsync().Result;
                        _logger.LogInformation($"Payload Get to Database: {programDataJson}");

                        if (!string.IsNullOrEmpty(programDataJson))
                        {

                            List<ProgramData> programDataList = JsonConvert.DeserializeObject<List<ProgramData>>(programDataJson);
                            _logger.LogInformation($"Deserialization successful. Number of items: {programDataList.Count}");
                            return programDataList;
                        }
                    }
                    return null;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error in getting program data from API: " + ex.Message);
                return new List<ProgramData>();
            }
        }

        private User GetExistingUserFromApi(string userId)
        {
            try
            {
                using (HttpClient httpClient = new HttpClient())
                {
                    httpClient.BaseAddress = new Uri(apiUrl);
                    HttpResponseMessage response = httpClient.GetAsync($"users/{userId}").Result;

                    if (response.IsSuccessStatusCode)
                    {
                        string userJson = response.Content.ReadAsStringAsync().Result;
                        User existingUser = System.Text.Json.JsonSerializer.Deserialize<User>(userJson);

                        if (existingUser != null)
                        {
                            return existingUser;
                        }
                    }

                    return null;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error in getting user data from API: " + ex.Message);
                return null;
            }
        }

        private double GetNetworkSpeed(string processName)
        {
            try
            {
                // Get the network interfaces
                var networkInterfaces = System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces();

                // Filter interfaces that are up and have operational status
                var activeInterfaces = networkInterfaces
                    .Where(interfaceItem => interfaceItem.OperationalStatus == System.Net.NetworkInformation.OperationalStatus.Up)
                    .ToList();

                // Find the process by name
                var targetProcess = Process.GetProcessesByName(processName).FirstOrDefault();

                if (targetProcess == null)
                {
                    _logger.LogWarning($"Process with name {processName} not found.");
                    return 0.0; // Return 0.0 if the process is not found.
                }

                // Get the process ID
                int processId = targetProcess.Id;

                // Get the bytes sent by the process
                long bytesSentByProcess = activeInterfaces
                    .Select(interfaceItem => interfaceItem.GetIPv4Statistics().BytesSent)
                    .Sum();

                // Wait for a short interval (e.g., 1 second)
                Thread.Sleep(1000);

                // Get the new bytes sent by the process after the interval
                long newBytesSentByProcess = activeInterfaces
                    .Select(interfaceItem => interfaceItem.GetIPv4Statistics().BytesSent)
                    .Sum();

                // Calculate bytes sent by the process during the interval
                long bytesSentDuringInterval = newBytesSentByProcess - bytesSentByProcess;

                // Calculate network speed for sent bytes in bytes per second
                double bytesPerSecond = bytesSentDuringInterval / 1.0; // 1 second interval

                return bytesPerSecond;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error measuring network speed: {ex.Message}");
                return 0.0; // Return 0.0 in case of an error.
            }
        }


        private LocalProgramData GetLocalThresholdSettings(string userId, string programName)
        {
            try
            {
                using (HttpClient httpClient = new HttpClient())
                {
                    httpClient.BaseAddress = new Uri(apiUrl);
                    HttpResponseMessage response = httpClient.GetAsync($"users/{userId}/localProgramData/{programName}").Result;

                    if (response.IsSuccessStatusCode)
                    {
                        string localThresholdJson = response.Content.ReadAsStringAsync().Result;
                        return JsonConvert.DeserializeObject<LocalProgramData>(localThresholdJson);
                    }
                    return null;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error in getting local threshold settings: {ex.Message}");
                return null;
            }
        }
        private ThresholdSettings GetThresholdTypeSettings(string userId, string programName)
        {
            try
            {
                using (HttpClient httpClient = new HttpClient())
                {
                    httpClient.BaseAddress = new Uri(apiUrl);
                    HttpResponseMessage response = httpClient.GetAsync($"users/{userId}/thresholdTypeSettings/{programName}").Result;

                    if (response.IsSuccessStatusCode)
                    {
                        Console.WriteLine("We are at GetThresholdTypeSettings");
                        string ThresholdTypeJson = response.Content.ReadAsStringAsync().Result;
                        return JsonConvert.DeserializeObject<ThresholdSettings>(ThresholdTypeJson);
                    }
                    return null;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error in getting local threshold settings: {ex.Message}");
                return null;
            }
        }

        private GlobalProgramData GetGlobalThresholdSettings(string programName)
        {
            try
            {
                using (HttpClient httpClient = new HttpClient())
                {
                    httpClient.BaseAddress = new Uri(apiUrl);
                    HttpResponseMessage response = httpClient.GetAsync($"GlobalPrograms/globalprograms/{programName}").Result;

                    if (response.IsSuccessStatusCode)
                    {
                        string globalThresholdJson = response.Content.ReadAsStringAsync().Result;
                        return JsonConvert.DeserializeObject<GlobalProgramData>(globalThresholdJson);
                    }
                    return null;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error in getting global threshold settings: {ex.Message}");
                return null;
            }
        }
        private void SendAnalysisToApi(Analysis Analysis)
        {
            try
            {
                using (HttpClient httpClient = new HttpClient())
                {
                    httpClient.BaseAddress = new Uri(apiUrl);

                    string analysisJson = System.Text.Json.JsonSerializer.Serialize(Analysis); ;
                    _logger.LogInformation($"Payload sent to API: {analysisJson}");

                    HttpResponseMessage postResponse = httpClient.PostAsync("api/Analyses", new StringContent(analysisJson, Encoding.UTF8, "application/json")).Result;

                    if (postResponse.IsSuccessStatusCode)
                    {
                        _logger.LogInformation($"User program data added or updated successfully for UserId: {Analysis.UserId}");
                    }
                    else
                    {
                        _logger.LogError($"Failed to post user program data. Status Code: {postResponse.StatusCode}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error sending data to API: {ex.Message}");
            }
        }

        private void SendDataToApi(User user)
        {
            try
            {
                using (HttpClient httpClient = new HttpClient())
                {
                    httpClient.BaseAddress = new Uri(apiUrl);

                    string userJson = System.Text.Json.JsonSerializer.Serialize(user); ;
                    _logger.LogInformation($"Payload sent to API: {userJson}");

                    HttpResponseMessage postResponse = httpClient.PostAsync("users/", new StringContent(userJson, Encoding.UTF8, "application/json")).Result;

                    if (postResponse.IsSuccessStatusCode)
                    {
                        _logger.LogInformation($"User program data added or updated successfully for UserId: {user.UserId}");
                    }
                    else
                    {
                        _logger.LogError($"Failed to post user program data. Status Code: {postResponse.StatusCode}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error sending data to API: {ex.Message}");
            }
        }


        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker Running at time:", DateTimeOffset.Now);
                await Task.Delay(1000, stoppingToken); //the delay (1000 ms = 1 second)
            }
        }
    }
}