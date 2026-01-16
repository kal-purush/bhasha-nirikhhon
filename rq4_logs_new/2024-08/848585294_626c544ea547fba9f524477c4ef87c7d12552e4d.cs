using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using violaoapi.Data;
using violaoapi.DTOs.Usuario.ResetSenha;
using violaoapi.Services.Auth;
using violaoapi.Services.Email;

namespace violaoapi.Controllers
{
    [Route("api/[controller]")]
    public class ResetSenhaController : ControllerBase
    {
        private readonly AuthService _authService;
        private readonly ApplicationDbContext _context;
        private readonly EmailService _emailService;

        public ResetSenhaController(AuthService authService, ApplicationDbContext context, EmailService emailService)
        {
            _authService = authService;
            _context = context;
            _emailService = emailService;
        }

        [HttpPost("reset-password")]
        public IActionResult ResetPassword([FromBody] ResetPassWordDTO request)
        {
            var principal = _authService.ValidatePasswordResetToken(request.Token);
            if (principal == null)
                return BadRequest("Token inválido ou expirado.");

            var userIdClaim = principal.Claims.FirstOrDefault(c => c.Type == "id");
            if (userIdClaim == null)
                return BadRequest("Token inválido.");

            int userId = int.Parse(userIdClaim.Value);
            var usuario = _context.Usuarios.FirstOrDefault(u => u.Id == userId);

            if (usuario != null)
            {
                usuario.Senha = BCrypt.Net.BCrypt.HashPassword(request.NewPassword);
                _context.SaveChanges();
            }

            return Ok("Senha redefinida com sucesso.");
        }

        [HttpPost("forgot-password")]
        public async Task<IActionResult> ForgotPassword([FromBody] ForgotPasswordDTO request)
        {

            var usuario = _context.Usuarios.FirstOrDefault(u => u.Email == request.Email);
            if (usuario == null)
                return BadRequest("Usuário não encontrado.");


            var token = _authService.GeneratePasswordResetToken(usuario);

            await _emailService.SendPasswordResetEmail(usuario.Email, token);

            return Ok("Email de recuperação de senha enviado.");
        }

    }
}