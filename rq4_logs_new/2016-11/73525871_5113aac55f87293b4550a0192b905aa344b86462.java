package Automaton;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Automaton {

	private Alphabet alphabet;
	private TreeSet<State> states;
	private String startStateLabel;
	private Set<String> endStateLabels;

	private String curStateLabel;

	private Set<StateChangeListener> listeners;

	public Automaton(Alphabet alphabet, TreeSet<State> states, String startStateLabel, Set<String> endStateLabels) {
		this.alphabet = alphabet;
		this.states = states;
		this.startStateLabel = startStateLabel;
		this.endStateLabels = endStateLabels;
		curStateLabel = "";
		listeners = new HashSet<>();
	}

	public Automaton() {
		this(new Alphabet(), new TreeSet<>(), "", new HashSet<>());
	}

	public Alphabet getAlphabet() {
		return alphabet;
	}

	public void setAlphabet(Alphabet alphabet) {
		this.alphabet = alphabet;
	}

	public void addState(State state) {
		states.add(state);
	}

	public TreeSet<State> getStates() {
		return states;
	}

	public void setStartState(String label) {
		if (!states.contains(new State(label)))
			throw new InvalidParameterException("startState must be in states");
		startStateLabel = label;
	}

	public void addEndState(String label) {
		if (!states.contains(new State(label)))
			throw new InvalidParameterException("endState must be one of the states");
		endStateLabels.add(label);
	}

	public void registerListener(StateChangeListener listener) {
		listeners.add(listener);
	}

	public void unregisterListener(StateChangeListener listener) {
		listeners.remove(listener);
	}

	public boolean processWord(String word) {
		if (startStateLabel == "")
			throw new RuntimeException("startState must be set");

		char[] c = word.toCharArray();
		curStateLabel = startStateLabel;
		for (char d : c) {
			State curState = findState(curStateLabel);
			String nextState = curState.getNextStateFor(d);
			if (nextState == null)
				return false; // throw new RuntimeException("the dea isn't
								// complete"); Just assuming it should end in a
								// Trap
			curStateLabel = nextState;
			for (StateChangeListener list : listeners) {
				list.onStateChange(curState.getLabel(), d, curStateLabel);
			}
		}
		if (endStateLabels.contains(curStateLabel))
			return true;
		return false;
	}

	private State findState(String label) {
		for (Iterator<State> iterator = states.iterator(); iterator.hasNext();) {
			State next = iterator.next();
			if (next.getLabel().equals(label))
				return next;
		}
		return null;
	}

	public static Automaton parseFromXml(File file) {
		Automaton au = new Automaton();

		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();

			Document doc = builder.parse(new FileInputStream(file));

			Element automaton = doc.getDocumentElement();
			if (!automaton.getTagName().toLowerCase().equals("automaton"))
				throw new RuntimeException("No valid xml: root element ist AUTOMATRON");
			if (!automaton.getElementsByTagName("TYPE").item(0).getAttributes().getNamedItem("value").getNodeValue()
					.equals("DEA"))
				throw new RuntimeException("Only DEAs supported yet.");

			// TODO: alphabet

			NodeList nList = automaton.getElementsByTagName("STATE");
			for (int i = 0; i < nList.getLength(); i++) {
				Node n = nList.item(i);
				if (n.getNodeType() == Element.ELEMENT_NODE) {
					Element e = (Element) n;

					State s = new State(e.getAttribute("name"));

					NodeList nListT = e.getElementsByTagName("TRANSITION");
					for (int o = 0; o < nListT.getLength(); o++) {
						Node nT = nListT.item(o);
						if (nT.getNodeType() == Element.ELEMENT_NODE) {
							Element eT = (Element) nT;

							String nextState = eT.getAttribute("target");
							List<Character> cS = new ArrayList<>();

							NodeList nListL = eT.getElementsByTagName("LABEL");
							for (int p = 0; p < nListL.getLength(); p++) {
								Node nL = nListL.item(p);
								if (nL.getNodeType() == Element.ELEMENT_NODE) {
									Element eL = (Element) nL;

									cS.add(eL.getAttribute("read").charAt(0));
								}
							}

							s.addTransition(nextState, cS);
						}
					}

					au.addState(s);

					if (e.getAttribute("finalstate").equals("true"))
						au.addEndState(s.getLabel());
				}

				au.setStartState(automaton.getElementsByTagName("INITIALSTATE").item(0).getAttributes()
						.getNamedItem("value").getNodeValue());

			}

		} catch (ParserConfigurationException | SAXException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return au;
	}

	public static void main(String[] args) {
		/*
		 * Automaton a = new Automaton();
		 * 
		 * Alphabet alph = new Alphabet(Arrays.asList('a', 'b'));
		 * a.setAlphabet(alph);
		 * 
		 * State s1 = new State("s1"); s1.addTransition("s2",
		 * Arrays.asList('a')); a.addState(s1);
		 * 
		 * State s2 = new State("s2"); s2.addTransition("s1",
		 * Arrays.asList('b')); a.addState(s2);
		 * 
		 * a.addEndState("s2"); a.setStartState("s1");
		 * 
		 * a.registerListener((prev, c, now) -> System.out.println(prev + " -> "
		 * + c + " -> " + now));
		 * 
		 * System.out.println(a.processWord("abababab"));
		 */

		Automaton a = Automaton.parseFromXml(new File("C:\\Users\\Laptop\\Documents\\testDEA.xml"));

		a.registerListener((prev, c, now) -> System.out.println(prev + " -> " + c + " -> " + now));

		System.out.println(a.processWord("ABABA"));
	}

}