package com.guideme.pattern;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.reflections.Reflections;

import javafx.util.Pair;

interface Xerox {
	void makeCopy();
}

class DigitalXerox implements Xerox {
	@Override
	public void makeCopy() {
		System.out.println("Make Copy from Digital Xerox");
	}
}

class InjectXerox implements Xerox {
	@Override
	public void makeCopy() {
		System.out.println("Make Copy from Inject Xerox");
	}
}

interface XeroxMachineFactory {
	Xerox makeCopy(int numberOfCopies);
}

class DigitalXeroxFactory implements XeroxMachineFactory {
	@Override
	public Xerox makeCopy(int numberOfCopies) {
		System.out.println("Make Copy from Digital Xerox and # of copies :" + numberOfCopies);
		return new DigitalXerox();
	}
}

class InjectXeroxFactory implements XeroxMachineFactory {
	@Override
	public Xerox makeCopy(int numberOfCopies) {
		System.out.println("Make Copy from Digital Xerox and # of copies :" + numberOfCopies);
		return new InjectXerox();
	}
}

class BuildMachine {
	
	List<Pair<String, XeroxMachineFactory>> factories = new ArrayList<>();

	public BuildMachine() throws Exception {
		Set<Class<? extends XeroxMachineFactory>> types = new Reflections("").getSubTypesOf(XeroxMachineFactory.class);
		for (Class<? extends XeroxMachineFactory> type : types) {
			factories.add(new Pair<>(type.getSimpleName().replaceAll("XeroxFactory", ""), type.getDeclaredConstructor().newInstance()));
		}
	}

	public Xerox makeCopies() throws Exception {
		for (int i = 0; i < factories.size(); i++) {
			System.out.println("Available Machines :" +i+" :"+ factories.get(i).getKey());
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String s;
			int i, amount;
			if ((s = reader.readLine()) != null && (i = Integer.parseInt(s)) >= 0 && i < factories.size()) {
				System.out.println("Number of Copies :");
				s = reader.readLine();
				amount = Integer.parseInt(s);
				return factories.get(i).getValue().makeCopy(amount);
			}
		}
	}

}
public class AbstractFactiryPatternExample {

	public static void main(String[] args) throws Exception{
		BuildMachine bm = new BuildMachine();
		Xerox xmf = bm.makeCopies();
		xmf.makeCopy();
	}

}