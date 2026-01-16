package uci.ics.mondego.tldr.extractor;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.FieldVisitor;

public class FieldVisitorImpl implements FieldVisitor{

	public AnnotationVisitor visitAnnotation(String arg0, boolean arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	public void visitAttribute(Attribute arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.toString()+"  here ");
	}

	public void visitEnd() {
		// TODO Auto-generated method stub
		
	}

	
}