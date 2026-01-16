package neuralnetwork.basenetwork;

import helpers.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import neuralnetwork.HiddenNode;
import neuralnetwork.InputNode;
import neuralnetwork.OutputNode;



public class Topology {
	private List<InputNode> inputNodes = new ArrayList<InputNode>();
	private List<HiddenNode> hiddenNodes = new ArrayList<HiddenNode>();
	private List<OutputNode> outputNodes = new ArrayList<OutputNode>();
	private List<Arc> arcs = new ArrayList<Arc>();
	
	protected Iterable<Arc> arcIterator() {
		return new Iterable<Arc>() {
			@Override
			public Iterator<Arc> iterator() {
				return Collections.unmodifiableCollection(arcs).iterator();
			}
		};
	}
	
	protected Iterable<InputNode> inputIterator() {
		return new Iterable<InputNode>() {
			@Override
			public Iterator<InputNode> iterator() {
				return Collections.unmodifiableCollection(inputNodes).iterator();
			}
		};
	}
	
	protected Iterable<HiddenNode> hiddenIterator() {
		return new Iterable<HiddenNode>() {
			@Override
			public Iterator<HiddenNode> iterator() {
				return Collections.unmodifiableCollection(hiddenNodes).iterator();
			}
		};
	}
	
	protected Iterable<OutputNode> outputIterator() {
		return new Iterable<OutputNode>() {
			@Override
			public Iterator<OutputNode> iterator() {
				return Collections.unmodifiableCollection(outputNodes).iterator();
			}
		};
	}
	
	public InputNode addInput(InputNode node) {
		this.inputNodes.add(node);
		return node;
	}
	
	public HiddenNode addHidden(HiddenNode node) {
		this.hiddenNodes.add(node);
		return node;
	}
	
	public OutputNode addOutput(OutputNode node) {
		this.outputNodes.add(node);
		return node;
	}
	
	public void addArc(Node parent, Node child) {
		this.arcs.add(new Arc(parent, child));
	}
	
	public int inputSize() {
		return this.inputNodes.size();
	}
	
	public int hiddenSize() {
		return this.hiddenNodes.size();
	}
	
	public int outputSize() {
		return this.outputNodes.size();
	}
	
	public int arcSize() {
		return this.arcs.size();
	}
}