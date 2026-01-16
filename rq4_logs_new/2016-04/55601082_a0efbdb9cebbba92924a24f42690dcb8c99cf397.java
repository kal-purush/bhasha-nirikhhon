package astar;

import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import astar.node.AstarNode;
import worldElement.WorldElement;
import worldGrid.WorldGrid;


public abstract class AstarAlg{
	public static List<WorldElement> findPath(WorldElement start, WorldElement goal, WorldGrid world){
		PriorityQueue<AstarNode> open = new PriorityQueue<AstarNode>();
		List<AstarNode> closed = new LinkedList<AstarNode>();
		open.add(new AstarNode(start, null, 0, heuristic(start, goal)));
		AstarNode current = null;
		while( !open.isEmpty()){
			current = open.poll();
			if(current.getElement().equals(goal)){
				break;
			}
			closed.add(current);
			for(AstarNode neigh : getNeighbours(current, goal, world)){
				if(open.contains(neigh)){
					boolean remove = false;
					for(AstarNode an : open){
						if(an.equals(neigh) && neigh.getDistance() < an.getDistance()){
							remove = true;
							break;
						}
					}
					if(remove){
						open.remove(neigh);
					}
				}
				if(closed.contains(neigh) && neigh.getDistance() < closed.get(closed.indexOf(neigh)).getDistance()){
					closed.remove(neigh);
				}
				if( !open.contains(neigh) && !closed.contains(neigh)){
					open.add(neigh);
				}
			}
		}
		return constructPath(current);
	}
	
	private static List<WorldElement> constructPath(AstarNode last){
		LinkedList<WorldElement> result = new LinkedList<WorldElement>();
		for(AstarNode n = last; n != null; n = n.getPrev()){
			result.addFirst(n.getElement());
		}
		return result;
	}
	
	private static List<AstarNode> getNeighbours(AstarNode current, WorldElement goal, WorldGrid grid){
		List<AstarNode> result = new LinkedList<AstarNode>();
		for(WorldElement e : grid.getNeighbours(current.getElement())){
			result.add(new AstarNode(e, current, current.getDistance() + 1, heuristic(e, goal)));
		}
		return result;
	}
	
	private static int heuristic(WorldElement node, WorldElement goal){
		int dx = Math.abs(node.getX() - goal.getX());
		int dy = Math.abs(node.getY() - goal.getY());
		return dx + dy;
	}
}