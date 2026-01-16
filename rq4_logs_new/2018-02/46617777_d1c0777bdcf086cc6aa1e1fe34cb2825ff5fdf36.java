package org.metaborg.lang.tiger.interpreter.natives;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Supplier;
import org.metaborg.org.metaborg.lang.tiger.interpreter.generated.terms.*;
import org.metaborg.meta.lang.dynsem.interpreter.nabl2.NaBL2TermBuild;
import org.metaborg.meta.lang.dynsem.interpreter.nodes.building.TermBuild;
import org.metaborg.meta.nabl2.interpreter.*;
import org.metaborg.meta.nabl2.stratego.*;
import org.metaborg.meta.nabl2.terms.*;
import org.metaborg.meta.nabl2.terms.generic.*;
import org.spoofax.interpreter.terms.IStrategoTerm;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.source.SourceSection;

@SuppressWarnings("unused")
@NodeChild(value = "term", type = TermBuild.class)
public abstract class nabl2_phantomIndex_1 extends TermBuild {
	private static int next = -1;

	public nabl2_phantomIndex_1(SourceSection source) {
		super(source);
	}

	@Specialization
	public ITermIndexTerm doGet(TermIndex_2_Term index) {
		return new TermIndex_2_Term(index.get_1(), next--);
	}

	public static TermBuild create(SourceSection source, TermBuild term) {
		return nabl2_phantomIndex_1NodeGen.create(source, term);
	}
}