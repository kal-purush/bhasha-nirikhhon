package br.com.caelum.viagens.voos.utils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.AllDirectedPaths;
import org.jgrapht.graph.DefaultDirectedGraph;

import br.com.caelum.viagens.administrativo.model.Aeroporto;
import br.com.caelum.viagens.support.Route;

public class GrafoRotasUtils {
	
	public static boolean temSequenciaLogica(List<Route> rotas) {
		return getCaminhos(rotas).stream().filter(path -> path.getLength() == rotas.size()).findAny().isPresent();
	}
	
	public static List<GraphPath<Aeroporto, Route>> getCaminhos(List<Route> rotas){
		AllDirectedPaths<Aeroporto, Route> caminhos = getAllCaminhosDiretos(rotas);
		return caminhos.getAllPaths(getAllOrigens(rotas), getAllPernaFinalCandidates(rotas), true, rotas.size());
	}
	
	public static List<GraphPath<Aeroporto, Route>> getCaminhos(List<Route> rotas, Aeroporto origem, Aeroporto destino){
		AllDirectedPaths<Aeroporto, Route> caminhos = getAllCaminhosDiretos(rotas);
		return caminhos.getAllPaths(origem, destino, true, rotas.size());
	}
	
	public static Aeroporto getDestino(List<Route> rotas) {
		return getAllDestinos(rotas).stream()
				.filter(a -> !getAllOrigens(rotas).contains(a)).collect(Collectors.toSet()).iterator().next();
	}
	
	public static Aeroporto getOrigem(List<Route> rotas) {
		return getAllOrigens(rotas).stream()
				.filter(a -> !getAllDestinos(rotas).contains(a)).collect(Collectors.toSet()).iterator().next();
	}
	
	private static AllDirectedPaths<Aeroporto, Route> getAllCaminhosDiretos(List<Route> rotas){
		Graph<Aeroporto, Route> grafoRotas = getGrafoDeRotas(rotas);
		return new AllDirectedPaths<>(grafoRotas);
	}
	
	private static Graph<Aeroporto, Route> getGrafoDeRotas(List<Route> rotas) {
		Graph<Aeroporto, Route> grafoRotas = new DefaultDirectedGraph<>(Route.class);
		rotas.forEach(r -> {
			grafoRotas.addVertex(r.getOrigem());
			grafoRotas.addVertex(r.getDestino());
			grafoRotas.addEdge(r.getOrigem(), r.getDestino(), r);
		});
		return grafoRotas;
	}

	private static Set<Aeroporto> getAllOrigens(List<Route> rotas) {
		return rotas.stream().map(r -> r.getOrigem()).collect(Collectors.toSet());
	}

	private static Set<Aeroporto> getAllDestinos(List<Route> rotas) {
		return rotas.stream().map(r -> r.getDestino()).collect(Collectors.toSet());
	}

	private static Set<Aeroporto> getAllPernaFinalCandidates(List<Route> rotas) {
		return getAllDestinos(rotas).stream().filter(a -> !getAllOrigens(rotas).contains(a)).collect(Collectors.toSet());
	}
}