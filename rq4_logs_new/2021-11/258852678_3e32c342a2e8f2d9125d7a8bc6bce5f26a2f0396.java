package org.ewn.jaxb;

import java.math.BigInteger;
import java.util.List;

/**
 * Dump utils
 */
public class Dump
{
	public static void dumpLex(LexicalEntry lexEntry, CharSequence indent, boolean silent)
	{
		if (!silent)
		{
			System.out.printf("%slexentry %s%n", indent, Strings.toString(lexEntry));
		}
	}

	public static void dumpSense(Sense sense, CharSequence indent, boolean silent)
	{
		String id = sense.getId();
		BigInteger n = sense.getN();
		if (!silent)
		{
			System.out.printf("%ssense id: %s%n", indent, id);
			System.out.printf("%s\tsensekey: %s%n", indent, Utils.toSensekey(id));
			System.out.printf("%s\tn: %s%n", indent, n == null ? "" : n);
		}

		// lexical entry
		LexicalEntry lex = (LexicalEntry) sense.getParent();
		Lemma lemma = lex.getLemma();
		assert lemma != null;
		if (!silent)
		{
			System.out.printf("%s\tlemma: %s%n", indent, lemma.getWrittenForm());
		}

		// adj
		AdjPositionType adjposition = sense.getAdjposition();
		if (!silent)
		{
			System.out.printf("%s\tadjPosition: %s%n", indent, adjposition);
		}

		// verb frames and templates
		List<Object> subcats = sense.getSubcat();
		for (Object subcat : subcats)
		{
			SyntacticBehaviour verbFrame = (SyntacticBehaviour) subcat;
			assert verbFrame != null;
			if (!silent)
			{
				System.out.printf("%s\tverb frame : %s = %s%n", indent, verbFrame.getId(), verbFrame.getSubcategorizationFrame());
			}
		}

		// lex relations
		for (SenseRelation senseRelation : sense.getSenseRelation())
		{
			Sense target = (Sense) senseRelation.getTarget();
			if (target != null) // local ref within this file
			{
				SenseRelationType type = senseRelation.getRelType();
				assert type != null;
				LexicalEntry targetLexEntry = Utils.getLexicalEntry(target);
				String targetLemma = targetLexEntry.getLemma().getWrittenForm();
				Synset targetSynset = (Synset) target.getSynset();
				if (!silent)
				{
					System.out.printf("%s\trelation: %s to target lemma '%s' synset '%s'%n", indent, type, targetLemma, targetSynset.getDefinition().get(0).getContent());
				}
			}
		}

		// synset
		Synset synset = (Synset) sense.getSynset();
		if (!silent)
		{
			System.out.printf("%s\tsynset %s%n", indent, Strings.toString(synset));
		}
	}

	public static void dumpSynset(Synset synset, CharSequence indent, boolean silent)
	{
		if (!silent)
		{
			System.out.printf("%s\tsynset id: %s%n", indent, synset.getId());
		}
		// members
		List<Object> lexObjects = synset.getMembers();
		assert lexObjects.size() > 0;
		for (Object lexObject : lexObjects)
		{
			LexicalEntry lex = (LexicalEntry) lexObject;
			Lemma lemma = lex.getLemma();
			String pronunciation = Utils.join(lex.getLemma().getPronunciation(), ' ', p -> {

				String ipa = p.getContent();
				String variety = p.getVariety();
				return String.format("%s/%s/", variety == null ? "" : "[" + variety + "] ", ipa);

			});
			if (!silent)
			{
				System.out.printf("%s\tmember: %s '%s' %s%n", indent, lex.getId(), lex.getLemma().getWrittenForm(), pronunciation);
			}
		}

		// definition
		Definition definition = synset.getDefinition().get(0);
		if (!silent)
		{
			System.out.printf("%s\tdefinition '%s'%n", indent, definition.getContent());
		}

		// example
		for (Example example : synset.getExample())
		{
			if (!silent)
			{
				System.out.printf("%s\texample '%s'%n", indent, example.getContent());
			}
		}

		// relation
		for (SynsetRelation synsetRelation : synset.getSynsetRelation())
		{
			Synset target = (Synset) synsetRelation.getTarget();
			if (target != null) // local ref within this file
			{
				SynsetRelationType type = synsetRelation.getRelType();
				assert type != null;
				if (!silent)
				{
					System.out.printf("%s\tsynset relation %s '%s'%n", indent, type, target.getDefinition().get(0).getContent());
				}
			}
		}
	}
}