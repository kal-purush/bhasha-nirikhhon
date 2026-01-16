package br.com.system.gestaoConstrucaoCivil.controllerTest;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.TestRestTemplate;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;

import br.com.system.gestaoConstrucaoCivil.GestaoConstrucaoCivilApplication;
import br.com.system.gestaoConstrucaoCivil.entity.Categoria;
import br.com.system.gestaoConstrucaoCivil.entity.Fornecedor;
import br.com.system.gestaoConstrucaoCivil.entity.Produto;
import br.com.system.gestaoConstrucaoCivil.entity.UnidadeMedida;
import br.com.system.gestaoConstrucaoCivil.repository.CargoRepository;
import br.com.system.gestaoConstrucaoCivil.repository.CategoriaRepository;
import br.com.system.gestaoConstrucaoCivil.repository.DadoEmpresaRepository;
import br.com.system.gestaoConstrucaoCivil.repository.EmpreendimentoRepository;
import br.com.system.gestaoConstrucaoCivil.repository.EnderecoRepository;
import br.com.system.gestaoConstrucaoCivil.repository.FornecedorRepository;
import br.com.system.gestaoConstrucaoCivil.repository.FuncionarioRepository;
import br.com.system.gestaoConstrucaoCivil.repository.ImovelRepository;
import br.com.system.gestaoConstrucaoCivil.repository.PessoaRepository;
import br.com.system.gestaoConstrucaoCivil.repository.PrestadoraServicoRepository;
import br.com.system.gestaoConstrucaoCivil.repository.ProdutoRepository;
import br.com.system.gestaoConstrucaoCivil.repository.TipoEmpreendimentoRepository;
import br.com.system.gestaoConstrucaoCivil.repository.UnidadeMedidaRepository;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = GestaoConstrucaoCivilApplication.class)
@WebIntegrationTest
public class ControllerTest {

	@Autowired
	private CargoRepository cargoRepository;
	@Autowired
	private CategoriaRepository categoriaRepository;
	@Autowired
	private DadoEmpresaRepository dadosEmpresaRepository;
	@Autowired
	private EnderecoRepository enderecoRepository;
	@Autowired
	private TipoEmpreendimentoRepository tipoEmpreendimentoRepository;
	@Autowired
	private ImovelRepository imovelRepository;
	@Autowired
	private FornecedorRepository fornecedorRepository;
	@Autowired
	private FuncionarioRepository funcionarioRepository;
	
	@Autowired
	private PrestadoraServicoRepository prestadoraServicoRepository;
	@Autowired
	private EmpreendimentoRepository empreendimentoRepository;
	@Autowired
	private PessoaRepository pessoaRepository;
	@Autowired
	private ProdutoRepository protudoRepository;
	@Autowired
	private UnidadeMedidaRepository unidadeRepository;
	
	
	private RestTemplate restTemplate = new TestRestTemplate();

	/*
	 * @Test public void testCriarCargoApi() throws JsonProcessingException {
	 * 
	 * Cargo cargo = new Cargo(); cargo.setDescricao("Teste OK");
	 * 
	 * cargoRepository.save(cargo); }
	 */
	/*
	 * @Test public void testeCriarCategoriaAPI() throws JsonProcessingException
	 * {
	 * 
	 * Categoria categoria = new Categoria(); categoria.setDescricaoCategoria(
	 * "Descricao Categoria"); categoriaRepository.save(categoria);
	 * 
	 * }
	 */

	
	/*  @Test
	  public void testeCriarEnderecoAPI() throws JsonProcessingException{
		  Endereco endereco = new Endereco(); endereco.setBairro("Santo Antonio");
		  endereco.setCidade("Campinas"); endereco.setCep("132000-022");
		 endereco.setNumero(1010); endereco.setEstado("SP"); endereco.setRua(
		 "Av 11 de agosot"); enderecoRepository.save(endereco); }
	     */
	    
    /*
	@Test
	public void testeCriarDadosEmpresaAPI() throws JsonProcessingException {

		DadoEmpresa dadosEmpresa = new DadoEmpresa();
		dadosEmpresa.setNomeFantasia("Loja de Tinta Bandeira");
		dadosEmpresa.setCnpj("67.712.922/1003-11");
		dadosEmpresa.setInscricaoEstadual("9999.111.566.848");
		dadosEmpresa.setRazaoSocial("Bandeira LTDA");
		dadosEmpresa.setEmail("emailteste@terra.com");
		dadosEmpresa.setTelefone("1938710006");


		 Endereco endereco = new Endereco(); endereco.setBairro("Santo Antonio");
		  endereco.setCidade("Campinas"); endereco.setCep("132000-022");
		 endereco.setNumero(1010); endereco.setEstado("SP"); endereco.setRua(
		 "Av 11 de agosot"); 
		 enderecoRepository.save(endereco); 
		 
		
		dadosEmpresa.setEndereco(endereco);
		
		dadosEmpresaRepository.save(dadosEmpresa);
	}*/
	/*
	 * @Test public void testeCriarTipoEmpreendimento() throws
	 * JsonProcessingException{
	 * 
	 * TipoEmpreendimento tipoEmpreendimentoCasa = new TipoEmpreendimento();
	 * 
	 * tipoEmpreendimentoCasa.setDescricaoTipoEmpreendimento(
	 * "Condominio de casas");
	 * 
	 * 
	 * tipoEmpreendimentoRepository.save(tipoEmpreendimentoCasa); }
	 */
    /*
	@Test
	public void testeCriarFornecedor() throws JsonProcessingException {
		
		
		Fornecedor fornecedor = new Fornecedor();
		fornecedor.setAtivo(true);
		fornecedor.setContato("Lula da Silva");
		
		DadoEmpresa dadosEmpresa = new DadoEmpresa();
		dadosEmpresa.setNomeFantasia("Loja de Materiais");
		dadosEmpresa.setCnpj("87.712.933/8569-12");
		dadosEmpresa.setInscricaoEstadual("1111.111.566.848");
		dadosEmpresa.setRazaoSocial("Loja de Materiais LTDA");
		dadosEmpresa.setEmail("lula@terra.com");
		dadosEmpresa.setTelefone("89332320006");
		Endereco endereco = new Endereco(); 
		endereco.setRua("Rua Av do Sol"); 
		endereco.setBairro("Bairro Pele");
		endereco.setCidade("Valinhos"); 
		endereco.setCep("132222-111");
		endereco.setNumero(11); 
		endereco.setEstado("SP");
		
		enderecoRepository.save(endereco); 
        dadosEmpresa.setEndereco(endereco);
        dadosEmpresaRepository.save(dadosEmpresa);
		
        fornecedor.setDadosEmpresa(dadosEmpresa);
		fornecedor.setObservacao("Corinthians > Santos");
		fornecedorRepository.save(fornecedor);
	}*/
	
	/*@Test
	public void testeCriarProduto() throws JsonProcessingException {
		
		Produto produto = new Produto();
		produto.setAtivo(true);
		Categoria categoria = categoriaRepository.findOne(4L);
		
		produto.setCategoria(categoria);
		produto.setCodigoBarra("1231231231321");
		produto.setDescricao("Tinta");
		
		UnidadeMedida unidadeMedida =  unidadeRepository.findOne(109L);
		unidadeMedida.setDescricaoCategoria("KM");
		produto.setUnidadeMedida(unidadeMedida);
		produto.setUnidadeMedida(unidadeMedida);
		produto.setValorCompra(10000.0);
		List <Fornecedor> fornecedores = fornecedorRepository.findAll();
		produto.setFornecedores(fornecedores);
		protudoRepository.save(produto);
		
	}*/
	/*@Test
	public void testeCriarFornecedor() throws JsonProcessingException {
		
		Imovel imovel = new Imovel();
		
		imovel.setAndar(10);
		
		imovelRepository.save(imovel);
	}*/
	/*@Test
	public void testeCriarFuncionario() throws JsonProcessingException {
	
		Empreendimento empreendimento = empreendimentoRepository.findOne(83L);
		
		System.out.println("Descricao " + empreendimento.getDescricao());
		System.out.println("Teste " + empreendimento.getEnderecoEmpreendimento().getBairro());
	}*/
	/*@Test
	public void testeCriarFuncionario() throws JsonProcessingException {
	
		Funcionario funcionario = new Funcionario();
	    funcionario.setNomeCompleto("Jose da Silva");
	    funcionario.setCpf("12111111111");
	    funcionario.setRg("231231231231");
	    funcionario.setIdade(69);
	    funcionario.setAtivo(true);
		
	    Endereco endereco = new Endereco(); 
		endereco.setRua("Rua Pau da Bandeira"); 
		endereco.setBairro("Bairro Tororó");
		endereco.setCidade("Campinas"); 
		endereco.setCep("132000-999");
		endereco.setNumero(1011); 
		endereco.setEstado("SP");
		
		enderecoRepository.save(endereco); 
	    funcionario.setEndereco(endereco);
	    
	    funcionario.setDataNascimento(new Date());
	    funcionario.setTelefoneCelular("3888888");
	    funcionario.setTelefoneFixo("8979878997");
	    Cargo cargo = new Cargo();
		cargo.setDescricao("aux");
		cargoRepository.save(cargo);
		funcionario.setCargo(cargo);
		funcionario.setCarteiraTrabalho(4545645);
		Date dataAdmissao = new Date();
		funcionario.setDataAdmissao(dataAdmissao);
		funcionario.setEmpreendimento(null);
		pessoaRepository.save(funcionario);
		
	}*/
	/*@Test
	public void testeCriarPrestadoraServico() throws JsonProcessingException {
	
		PrestadoraServico prestadoraServico = new PrestadoraServico();
		DadoEmpresa dadosEmpresa = new DadoEmpresa();
		dadosEmpresa.setNomeFantasia("Empresa Eng");
		dadosEmpresa.setCnpj("90.888.111/1003-21");
		dadosEmpresa.setInscricaoEstadual("9988.122.567.748");
		dadosEmpresa.setRazaoSocial("Empresa Eng LTDA");
		dadosEmpresa.setEmail("eng@terra.com");
		dadosEmpresa.setTelefone("1938710076");
		Endereco endereco = new Endereco(); 
		endereco.setRua("Rua 100"); 
		endereco.setBairro("Bairro da Lua");
		endereco.setCidade("Campinas"); 
		endereco.setCep("132110-785");
		endereco.setNumero(41); 
		endereco.setEstado("SP");
		
		enderecoRepository.save(endereco); 
        dadosEmpresa.setEndereco(endereco);
        dadosEmpresaRepository.save(dadosEmpresa);
	    
        prestadoraServico.setDadoEmpresa(dadosEmpresa);
        prestadoraServicoRepository.save(prestadoraServico);
        
		
	}*/
	
	/*@Test
	public void testeCriarEmpreendimento() throws JsonProcessingException {
	
		Empreendimento empreendimento = new Empreendimento();
		empreendimento.setDescricao("Construcao OK");
		empreendimento.setAtivo(true);
		empreendimento.setCrea("45456456456");
		empreendimento.setDataAbertura(new Date());
		empreendimento.setDatafechamento(null);
		
		Endereco endereco = enderecoRepository.findOne(73L); 
		
		empreendimento.setEnderecoEmpreendimento(endereco);
        //Colocar o id do banco
        
		PrestadoraServico engenheiroResponsavelTerceiro = prestadoraServicoRepository.findOne(72L);
        empreendimento.setEngenheiroResponsavelTerceiro(engenheiroResponsavelTerceiro);
        Funcionario engenheiroResponsavelFuncionario = funcionarioRepository.findOne(68L);
        empreendimento.setEngenheiroResponsavelFuncionario(engenheiroResponsavelFuncionario);
        
        
        TipoEmpreendimento tipoEmpreendimento = tipoEmpreendimentoRepository.findOne(38L);
        empreendimento.setTipoEmpreendimento(tipoEmpreendimento);
        empreendimento.setValoresGastos(0.0);
        empreendimento.setValorMaximoGastar(100000000.0);
        empreendimentoRepository.save(empreendimento);
        
        
        
	}*/

}