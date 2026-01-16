package org.metaborg.lang.tiger.interpreter.natives;

import org.metaborg.meta.lang.dynsem.interpreter.nodes.building.TermBuild;
import org.metaborg.meta.lang.dynsem.interpreter.terms.ITerm;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.source.SourceSection;

@NodeChild(value = "term", type = TermBuild.class)
public abstract class _printSTerm_1 extends TermBuild {

	public _printSTerm_1(SourceSection source) {
		super(source);
	}

	@Specialization
	public ITerm doit(ITerm t) {
		System.out.println(t.getStrategoTerm());
		return t;
	}

	public static TermBuild create(SourceSection source, TermBuild print) {
		return _printSTerm_1NodeGen.create(source, print);
	}

}