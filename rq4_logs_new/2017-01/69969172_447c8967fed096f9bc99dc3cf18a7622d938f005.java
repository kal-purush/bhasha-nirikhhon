package de.thorbenkuck.rhfw.pipe;

import de.thorbenkuck.rhfw.annotations.RegisterModule;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;

class ClassDependencyResolver implements DependencyResolver {

	private DataOutputPipe dataOutputPipe;
	private ClassFactory classFactory;

	public ClassDependencyResolver(DataOutputPipe dataOutputPipe) {
		this.dataOutputPipe = dataOutputPipe;
		classFactory = new ClassFactory();
	}

	@Override
	public void resolve() {
		FastClasspathScanner fastClasspathScanner = new FastClasspathScanner();
		fastClasspathScanner.matchClassesWithAnnotation(RegisterModule.class, aClass -> {
			if(aClass.getAnnotation(RegisterModule.class).included()) {
				dataOutputPipe.add(aClass.getName(), classFactory.create(aClass));
			} else {
				// TODO: log
			}
		});
	}
}