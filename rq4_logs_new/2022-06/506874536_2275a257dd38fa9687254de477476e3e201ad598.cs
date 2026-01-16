using service;
class AtendenteService:IAtendenteService
{
	public String Saudacao(String nome)=> $"Olá, {nome}";
	public String ServicosOferecidos()
	{
		return "Vendas de Motos\nVendas de Computador";
	}
}