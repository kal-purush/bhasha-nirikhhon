using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SupportChat.ChatAPI.Models;
using SupportChat.ChatAPI.Services;

namespace SupportChat.ChatAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ChatController : ControllerBase
    {
        private readonly SessionQueueService _queueService;
        private readonly AgentAssignmentService _assignmentService;

        public ChatController(SessionQueueService queueService, AgentAssignmentService assignmentService)
        {
            _queueService = queueService;
            _assignmentService = assignmentService;
        }

        [HttpPost("create-session")]
        public IActionResult CreateSession([FromForm] string userId)
        {
            var session = new ChatSession
            {
                UserId = userId,
                LastPolledAt = DateTime.UtcNow
            };

            
            if (_queueService.Count() >= 24) // Hardcoded to 24
                return StatusCode(429, "No agents available at the moment.");

            _queueService.Enqueue(session);

            return Ok(new
            {
                session.Id,
                session.Status
            });
        }

        [HttpGet("queue")]
        public IActionResult GetQueue() => Ok(_queueService.GetAll());

        [HttpPost("poll/{sessionId}")]
        public IActionResult Poll(Guid sessionId)
        {
            var session = _queueService.GetById(sessionId);
            if (session == null) return NotFound("Session not found");

            session.LastPolledAt = DateTime.UtcNow;
            session.MissedPolls = 0;
            session.Status = SessionStatus.Active;

            return Ok(new { status = session.Status.ToString() });
        }

        [HttpPost("complete/{sessionId}")]
        public IActionResult CompleteSession(Guid sessionId)
        {
            var session = _queueService.GetById(sessionId);
            if (session == null) return NotFound();

            if (session.AssignedAgentId != null)
            {
                var agent = _assignmentService.FindAgentById(session.AssignedAgentId);
                if (agent != null)
                {
                    agent.AssignedSessionIds.Remove(sessionId.ToString());
                }
            }

            session.Status = SessionStatus.Closed;
            Console.WriteLine($"[System] Session {sessionId} completed and closed.");
            return Ok("Session closed.");
        }

    }
}