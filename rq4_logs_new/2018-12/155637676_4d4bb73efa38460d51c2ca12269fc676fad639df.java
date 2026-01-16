package uci.ics.mondego.tldr.extractor;

import org.objectweb.asm.AnnotationVisitor;

public class AnnotationVisitorImpl implements AnnotationVisitor{

	public void visit(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		System.out.println(arg0);
	}

	public AnnotationVisitor visitAnnotation(String arg0, String arg1) {
		// TODO Auto-generated method stub
		System.out.println(arg0+"   "+arg1);
		return null;
	}

	public AnnotationVisitor visitArray(String arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0);
		return null;
	}

	public void visitEnd() {
		// TODO Auto-generated method stub
		System.out.println("here");
	}

	public void visitEnum(String arg0, String arg1, String arg2) {
		// TODO Auto-generated method stub
		
		System.out.println(arg0+"  "+arg1+"  "+arg2);
		
	}
	

}