package org.metaborg.lang.tiger.interpreter.natives;

import org.metaborg.meta.lang.dynsem.interpreter.nodes.building.TermBuild;
import org.metaborg.meta.lang.dynsem.interpreter.terms.IListTerm;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.source.SourceSection;

@NodeChild(value = "listbuild", type = TermBuild.class)
public abstract class printL_1 extends TermBuild {

	public printL_1(SourceSection source) {
		super(source);
	}

	@Specialization
	public <T> IListTerm<T> doString(IListTerm<T> l) {
		System.out.print(l);

		return l;
	}

	public static TermBuild create(SourceSection source, TermBuild listbuild) {
		return printL_1NodeGen.create(source, listbuild);
	}
}