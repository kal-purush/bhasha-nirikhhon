package graph;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

public class Graph<E> {

	private Set<Vertex<E>> vertices = new HashSet<>();

	public Path<E> findShortestPath(Vertex<E> source, Vertex<E> dest) {
		List<Path<E>> allPaths = findAllPaths(source, dest);
		Path<E> result = allPaths.get(0);
		for (Path<E> path : allPaths) {
			if(path.getWeight().compareTo(result.getWeight()) < 0) {
				result = path;
			}
		}
		return result;
	}
	
	public Path<E> findShortestPathLambda(Vertex<E> source, Vertex<E> dest) {
		List<Path<E>> allPaths = findAllPaths(source, dest);

		return allPaths
				.stream()
				.min((a,b) -> a.getWeight().compareTo(b.getWeight()))
				.get();
	}	
	
	public List<Path<E>> findAllPaths(
			Vertex<E> source, 
			Vertex<E> target) {
		
		List<Path<E>> paths = new LinkedList<Path<E>>();
		findPaths(source, null, target, paths, new Path<E>());
		return paths;
	}

	private void findPaths(
			Vertex<E> current,
			Vertex<E>.Edge currentEdge, 
			Vertex<E> target, 
			List<Path<E>> paths,
			Path<E> path) {

		if (current.equals(target)) {
			paths.add(path.getCopy());
			path.getVertices().remove(currentEdge);
			return;
		}

		for (Vertex<E>.Edge edge : current.getNeighbors()) {
			path.getVertices().add(edge);
			findPaths(edge.getDestination(), edge, target, paths, path);
		}

		path.getVertices().remove(currentEdge);

	}	
	
	public boolean hasPath(
			Vertex<E> source, 
			Vertex<E> destination) {

		Set<Vertex<E>> visited = new HashSet<>();
		Queue<Vertex<E>> queue = new LinkedList<>();
		queue.add(source);
		visited.add(source);

		while (!queue.isEmpty()) {
			Vertex<E> vertex = queue.poll();
			if (vertex.equals(destination)) {
				return true;
			}
			Set<Vertex<E>.Edge> neighbors = vertex.getNeighbors();
			for (Vertex<E>.Edge edge : neighbors) {
				Vertex<E> neighbor = edge.getDestination();
				if (!visited.contains(neighbor)) {
					queue.add(neighbor);
					visited.add(neighbor);
				}
			}
		}
		return false;
	}	

	public void traverse(Vertex<E> source) {
		Set<Vertex<E>> visited = new HashSet<>();
		Queue<Vertex<E>> queue = new LinkedList<>();
		queue.add(source);
		visited.add(source);

		while (!queue.isEmpty()) {
			Vertex<E> vertex = queue.poll();
			System.out.println(vertex.getValue());
			Set<Vertex<E>.Edge> neighbors = vertex.getNeighbors();
			for (Vertex<E>.Edge edge : neighbors) {
				Vertex<E> neighbor = edge.getDestination();
				if (!visited.contains(neighbor)) {
					queue.add(neighbor);
					visited.add(neighbor);
				}
			}
		}
	}	
	
	public Vertex<E> addVertex(E value) {
		Vertex<E> vertex = new Vertex<>(value);
		vertices.add(vertex);
		return vertex;
	}

}