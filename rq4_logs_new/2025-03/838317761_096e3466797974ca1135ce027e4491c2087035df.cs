using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Connectors.Google;
using Microsoft.SemanticKernel.Connectors.OpenAI;
using WordFlux.Application.Common.Abstractions;

namespace WordFlux.Translations.Ai;

public class AutocompleteService : IAutocompleteService
{
    private readonly ILogger<AutocompleteService> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly IConfiguration _configuration;

    public AutocompleteService(IServiceProvider serviceProvider, ILogger<AutocompleteService> logger, IConfiguration configuration)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _configuration = configuration;
    }

    private KeyedKernelType KernelType => KeyedKernelType.GeminiFlash;
    
    private Kernel Kernel => _serviceProvider.ResolveKernel(KernelType);

    [Experimental("SKEXP0010")]
    public async Task<(string detectedLanguage, List<(string, string)> autocompletes)?> GetAutocompleteWithTranslations(string term, string lang1, string lang2, CancellationToken cancellationToken = default)
    {
        KernelArguments arguments = CreatePromptArguments(KernelType, term, lang1, lang2);
        
        var result = await AiFunctions.AutocompleteWithTranslationFunc.InvokeAsync<ChatMessageContent>(Kernel, arguments, cancellationToken);
        
        if (result == null || result.Content == null)
        {
            _logger.LogError("Got null result");

            return null;
        }

        var content = JsonSerializer.Deserialize<AutocompleteWithTranslationsResult>(result.Content);

        if (content == null)
        {
            return null;
        }

        return (content.DetectedLanguage, content.Autocompletes.Select(x => (x.AutocompleteResult, x.TranslatedAutocompleteResult)).ToList());
    }


    [Experimental("SKEXP0070")]
    private static KernelArguments CreatePromptArguments(KeyedKernelType kernelType, string term, string lang1, string lang2)
    {
        var provider = kernelType.GetProvider();

        if (provider == AiProvider.Gemini)
        {
            KernelArguments arguments = new(new GeminiPromptExecutionSettings
            {
                ResponseSchema = typeof(AutocompleteWithTranslationsResult),
                ResponseMimeType = "application/json",
                Temperature = 0.1
            }) { { "lang1", lang1 }, { "lang2", lang2 }, { "term", term } };

            return arguments;
        }

        if (provider == AiProvider.OpenAI)
        {
            KernelArguments arguments = new(new OpenAIPromptExecutionSettings
            {
                ResponseFormat = "json_object",
                Temperature = 0.1
            }) { { "lang1", lang1 }, { "lang2", lang2 }, { "term", term } };

            return arguments;
        }

        throw new NotImplementedException($"Provider {provider} not implemented");

    }
}



file class AutocompleteWithTranslationsResult
{
    [JsonPropertyName("autocompletes")] public List<AutocompleteTranslationItem> Autocompletes { get; set; }
    [JsonPropertyName("lang")] public string DetectedLanguage { get; set; }
}

file class AutocompleteTranslationItem
{
    [JsonPropertyName("term")] 
    public string AutocompleteResult { get; set; } 
        
    [JsonPropertyName("term_translated")] 
    public string TranslatedAutocompleteResult { get; set; } 
}