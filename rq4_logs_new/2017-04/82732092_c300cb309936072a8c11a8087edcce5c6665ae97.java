package kpi.labs.ai.lab1.graph;

import java.util.*;

/**
 * Created by 4an70m on 02.04.2017.
 */
public class BreadthFirstGraphProcessorV3 implements GraphProcessor{

    public void breadthFirstSearchForLongestLink(Graph graph) {
        List<Path> longestPathes = new ArrayList<>();
        longestPathes.addAll(
                breadthFirstSearchMaxLength(
                        graph.getNodeByValue(4),
                        graph.getNodeByValue(3))
        );
        System.out.println(getLongestPath(longestPathes));

    }

    private Queue<Path> breadthFirstSearchMaxLength(Node headNode, Node nodeToSearch) {
        PathManager pathManager = new PathManager();
        Queue<Node> agenda = new LinkedList<>();
        agenda.add(headNode);

        while (!agenda.isEmpty()) {
            Node node = (Node) agenda.remove();
            for ( NodeRelation childRelation : node.getRelations()) {
                Path selectedPath = pathManager.add(childRelation);
                if (!selectedPath.containsRelation(childRelation)) {
                    agenda.add(childRelation.getChildNode());
                }

            }
        }
        return pathManager.pathes;
    }

    public class PathManager {
        public Queue<Path> pathes;

        public PathManager(){
            pathes = new LinkedList<>();
        }

        public Path add (NodeRelation relation) {
            Path selectedPath = null;
            if (pathes.isEmpty()) {
                selectedPath = new Path();
                selectedPath.add(relation);
                pathes.add(selectedPath);
                return selectedPath;
            }
            for (Path path : new LinkedList<Path>(pathes)) {
                if (path.isFinished()) {
                    continue;
                }

                NodeRelation lastLink = path.getLastPathLink();
                if (lastLink.getChildNode().equals(relation.getParentNode())) {
                    selectedPath = new Path(path);
                    selectedPath.add(relation);
                    pathes.add(selectedPath);
                }
            }
            return selectedPath;
        }
    }

    private static List<Path> getLongestPath (Collection<Path> paths) {
        List<Path> longestPathList = new ArrayList<>();
        Path longestPath = new Path();
        for (Path path : paths) {
            if (path.isFinished()) {
                continue;
            }
            if (longestPath.getSize() < path.getSize()) {
                longestPath = path;
            }
        }
        for (Path path : paths) {
            if (path.isFinished()) {
                continue;
            }
            if (longestPath.getSize() == path.getSize()) {
                longestPathList.add(path);
            }
        }
        return longestPathList;
    }

}