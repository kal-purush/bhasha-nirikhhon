package br.com.dbc.vemser.ecommerce.doc;

import br.com.dbc.vemser.ecommerce.dto.usuario.LoginDTO;
import br.com.dbc.vemser.ecommerce.dto.usuario.UserAtualizacaoDTO;
import br.com.dbc.vemser.ecommerce.dto.usuario.UsuarioLogadoDTO;
import br.com.dbc.vemser.ecommerce.entity.UsuarioEntity;
import br.com.dbc.vemser.ecommerce.exceptions.RegraDeNegocioException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

public interface AuthControllerDoc {

    @Operation(summary = "Autenticar o login de acesso e retornar um token de acesso.", description = "Valida o login e retorna token de acesso correspondente ao nível de permissão do login.")
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "200", description = "Retorna um token JWT de autenticação."),
                    @ApiResponse(responseCode = "403", description = "Você não tem permissão para acessar este recurso."),
                    @ApiResponse(responseCode = "404", description = "Página não encontrada."),
                    @ApiResponse(responseCode = "500", description = "Foi gerada uma exceção.")
            }
    )
    @PostMapping
    String auth(@RequestBody @Valid LoginDTO loginDTO) throws RegraDeNegocioException;

    @Operation(summary = "Cadastra alternativo para admins.", description = "Serve para cadastrar um login com permimssão específica. APENAS ADMINS TEM ACESSO A ESSA ROTA.")
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "200", description = "Retorna as informações cadastradas sem criptografia."),
                    @ApiResponse(responseCode = "403", description = "Você não tem permissão para acessar este recurso."),
                    @ApiResponse(responseCode = "404", description = "Página não encontrada."),
                    @ApiResponse(responseCode = "500", description = "Foi gerada uma exceção.")
            }
    )
    @PostMapping("/cadastro")
    LoginDTO cadastro(@RequestBody LoginDTO user, @RequestParam(value = "role", required = false) Integer role) throws RegraDeNegocioException;

    @Operation(summary = "Retornar informações sobre o usuário logado.", description = "Retorna informações do usuário logado pelo token.")
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "200", description = "Retorna as informações cadastradas do usuário logado."),
                    @ApiResponse(responseCode = "403", description = "Você não tem permissão para acessar este recurso."),
                    @ApiResponse(responseCode = "404", description = "Página não encontrada."),
                    @ApiResponse(responseCode = "500", description = "Foi gerada uma exceção.")
            }
    )
    @GetMapping("/usuario-logado")
    UsuarioLogadoDTO recuperarUsuarioLogado() throws RegraDeNegocioException;

    @Operation(summary = "Alterar senha de usuário.", description = "Altera a senha de um usuário pelo JSON. APENAS ADMINS TEM ACESSO A ESSA ROTA.")
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "200", description = "Não possui retorno."),
                    @ApiResponse(responseCode = "403", description = "Você não tem permissão para acessar este recurso."),
                    @ApiResponse(responseCode = "404", description = "Página não encontrada."),
                    @ApiResponse(responseCode = "500", description = "Foi gerada uma exceção.")
            }
    )
    @PutMapping("/usuario-trocar-senha")
    ResponseEntity<Void> alterarSenhaDeUsuario(@RequestBody LoginDTO loginDTO) throws RegraDeNegocioException;

    @Operation(summary = "Desativar o usuário que está logado.", description = "Desativa o usuário logado pegando como referência o seu token.")
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "200", description = "Não possui retorno."),
                    @ApiResponse(responseCode = "403", description = "Você não tem permissão para acessar este recurso."),
                    @ApiResponse(responseCode = "404", description = "Página não encontrada."),
                    @ApiResponse(responseCode = "500", description = "Foi gerada uma exceção.")
            }
    )
    @PutMapping("/desativar-usuario/{login}")
    ResponseEntity<Void> desativarUsuarioLogado(@NotBlank(message = "Informe o login.")
                                                       @PathVariable String login) throws RegraDeNegocioException;

    @Operation(summary = "Atualizar usuário pelo login.", description = "Atualiza um usuário pelo seu login. É passado um usuário com as alterações desejadas.")
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "200", description = "Não possui retorno."),
                    @ApiResponse(responseCode = "403", description = "Você não tem permissão para acessar este recurso."),
                    @ApiResponse(responseCode = "404", description = "Página não encontrada."),
                    @ApiResponse(responseCode = "500", description = "Foi gerada uma exceção.")
            }
    )
    @PutMapping("/atualizar-usuario/{login}")
    ResponseEntity<Void> atualizarUsuario(@NotBlank(message = "Informe o login.")
                                                 @PathVariable String login,
                                                 @RequestBody UserAtualizacaoDTO userAtualizacaoDTO) throws RegraDeNegocioException;

}