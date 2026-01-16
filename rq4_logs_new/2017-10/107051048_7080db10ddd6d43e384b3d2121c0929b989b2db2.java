package frogs;

import java.util.*;

public class FrogState {
	StringBuilder config;
	int spaceLocation;
	List<FrogState> children;
	FrogState parent;
	static int n;
	
	public FrogState(int spaceLocation){
		this.config = new StringBuilder(2*n+1);
		for(int i=0; i<n; i++){
			this.config.append('>');
		}
		this.config.append('_');
		for(int i=n+1; i<2*n+1; i++){
			this.config.append('<');
		}
		this.spaceLocation = spaceLocation;
		this.children = new ArrayList<FrogState>();
	}
	
	public FrogState(int spaceLocation, StringBuilder source, FrogState parent){
		this.config = source;
		this.spaceLocation = spaceLocation;
		this.children = new ArrayList<FrogState>();
		this.parent = parent;
	}
	
	public void findOptions(){
		if (this.spaceLocation > 0 && this.config.charAt(this.spaceLocation-1) == '>'){
			StringBuilder nextConfig = new StringBuilder(this.config.toString());
			int nextSpaceLocation = this.spaceLocation - 1;
			nextConfig.setCharAt(nextSpaceLocation, '_');
			nextConfig.setCharAt(this.spaceLocation, '>');
			this.children.add(new FrogState(nextSpaceLocation, nextConfig, this));
		}
		if (this.spaceLocation > 1 && this.config.charAt(this.spaceLocation-2) == '>'){
			StringBuilder nextConfig = new StringBuilder(this.config.toString());
			int nextSpaceLocation = this.spaceLocation - 2;
			nextConfig.setCharAt(nextSpaceLocation, '_');
			nextConfig.setCharAt(this.spaceLocation, '>');
			this.children.add(new FrogState(nextSpaceLocation, nextConfig, this));
		}
		if (this.spaceLocation < 2*n && this.config.charAt(this.spaceLocation+1) == '<'){
			StringBuilder nextConfig = new StringBuilder(this.config.toString());
			int nextSpaceLocation = this.spaceLocation + 1;
			nextConfig.setCharAt(nextSpaceLocation, '_');
			nextConfig.setCharAt(this.spaceLocation, '<');
			this.children.add(new FrogState(nextSpaceLocation, nextConfig, this));
		}
		if (this.spaceLocation < 2*n-1 && this.config.charAt(this.spaceLocation+2) == '<'){
			StringBuilder nextConfig = new StringBuilder(this.config.toString());
			int nextSpaceLocation = this.spaceLocation + 2;
			nextConfig.setCharAt(nextSpaceLocation, '_');
			nextConfig.setCharAt(this.spaceLocation, '<');
			this.children.add(new FrogState(nextSpaceLocation, nextConfig, this));
		}
		
		for (FrogState childState: this.children){
			childState.findOptions();
		}
	}
	
	public void printBackTrace(FrogState leafState){
		FrogState currState = leafState;
		Stack<FrogState> backTrace = new Stack<FrogState>();
		while (currState != null){
			backTrace.push(currState);
			currState = currState.parent;
		}
		while(!backTrace.isEmpty()){
			System.out.println(backTrace.pop().config);
		}
	}
	
	
	public static FrogState getSolutionDFS(FrogState rootState){
		Stack<FrogState> stack = new Stack<FrogState>();
		stack.push(rootState);
		while(!stack.isEmpty()){
			FrogState currState = stack.pop();
			if (currState.config.toString().matches("<{" + n + "}_>{" + n + "}")){
				currState.printBackTrace(currState);
				return currState;
			} 
			for (FrogState childState: currState.children){
				stack.push(childState);
			}
		}
		return null;
	}
	
	public static void main(String[] args){
		Scanner in = new Scanner(System.in);
		System.out.println("Enter the number of frogs from each side:");
		n = in.nextInt();
		
		FrogState rootState = new FrogState(n);
		rootState.findOptions();
		getSolutionDFS(rootState);
	}

}

