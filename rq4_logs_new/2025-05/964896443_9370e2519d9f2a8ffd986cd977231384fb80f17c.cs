using System;
using System.Threading.Tasks;
using AIPal.API.Models;
using AIPal.Services;
using AIPal.Services.Handlers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace AIPal.API.Controllers
{
    /// <summary>
    /// API controller for handling agent-based interactions with AIPal.
    /// Provides endpoints for processing requests through an AI agent that can use tools.
    /// </summary>
    [ApiController]
    [Route("api/[controller]")]
    public class AgentController : ControllerBase
    {
        private readonly IAgentService _agentService;
        private readonly AgentRequestProcessor _requestProcessor;
        private readonly ILogger<AgentController> _logger;

        /// <summary>
        /// Initializes a new instance of the AgentController.
        /// </summary>
        /// <param name="agentService">The agent service for processing requests</param>
        /// <param name="serviceProvider">The service provider for resolving handlers</param>
        /// <param name="logger">The logger</param>
        public AgentController(
            IAgentService agentService,
            IServiceProvider serviceProvider,
            ILogger<AgentController> logger)
        {
            _agentService = agentService ?? throw new ArgumentNullException(nameof(agentService));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _requestProcessor = new AgentRequestProcessor(serviceProvider, logger);
        }

        /// <summary>
        /// Processes a request through an AI agent that can use tools and perform multi-step reasoning.
        /// Uses the handler system to route requests to the appropriate handler.
        /// </summary>
        /// <param name="request">The request containing the message and context ID</param>
        /// <returns>The agent's response including any actions taken</returns>
        [HttpPost("process")]
        public async Task<ActionResult<AgentResponseDto>> ProcessRequest([FromBody] AgentRequestDto request)
        {
            if (string.IsNullOrWhiteSpace(request.Message))
            {
                return BadRequest(new { error = "Message cannot be empty" });
            }

            _logger.LogInformation("Received agent request: {Message}", request.Message);

            // Process the request through the handler system
            var agentResponse = await _requestProcessor.ProcessRequestAsync(request);
            
            // Convert the internal AgentResponse to the DTO for the API
            var responseDto = new AgentResponseDto
            {
                Response = agentResponse.Response,
                Success = agentResponse.Success,
                ErrorMessage = agentResponse.ErrorMessage
            };

            // Convert the actions
            foreach (var action in agentResponse.Actions)
            {
                responseDto.Actions.Add(new AgentActionDto
                {
                    ToolName = action.ToolName,
                    Input = action.Input,
                    Output = action.Output,
                    Success = action.Success
                });
            }

            return Ok(responseDto);
        }

        /// <summary>
        /// Gets information about all available tools that the agent can use.
        /// </summary>
        /// <returns>A list of available tools</returns>
        [HttpGet("tools")]
        public ActionResult<AgentToolInfoDto[]> GetAvailableTools()
        {
            var tools = _agentService.GetAvailableTools();
            var toolDtos = new AgentToolInfoDto[tools.Count];

            for (int i = 0; i < tools.Count; i++)
            {
                var tool = tools[i];
                toolDtos[i] = new AgentToolInfoDto
                {
                    Name = tool.Name,
                    Description = tool.Description,
                    Parameters = tool.Parameters
                };
            }

            return Ok(toolDtos);
        }
    }
}