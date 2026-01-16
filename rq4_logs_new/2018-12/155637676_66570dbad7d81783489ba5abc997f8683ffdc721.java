package uci.ics.mondego.tldr.extractor;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class ClassVisitorImpl implements ClassVisitor{

	
	public ClassVisitorImpl(){
		super();
	}
	
	public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
		// TODO Auto-generated method stub
		System.out.println("signature " + signature);
		System.out.println("class name : "+ name);
		System.out.println("super class name : "+ superName);
		
		
	}

	public AnnotationVisitor visitAnnotation(String org0, boolean arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	public void visitAttribute(Attribute arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visitEnd() {
		// TODO Auto-generated method stub
		
	}

	public FieldVisitor visitField(int access, String name,
			String desc, String signature, Object arg4) {
		// TODO Auto-generated method stub
		
		System.out.println(" " + name +"-------"+ desc);

		return null;
	}

	public void visitInnerClass(String arg0, String arg1, String arg2, int arg3) {
		// TODO Auto-generated method stub
		
	}

	public MethodVisitor visitMethod(int access, String name,
			String desc, String signature, String[] exceptions) {
		// TODO Auto-generated method stub
		
		System.out.println(" " + name +"-------"+ desc);
		
		return null;
	}

	public void visitOuterClass(String arg0, String arg1, String arg2) {
		// TODO Auto-generated method stub
		
	}

	public void visitSource(String arg0, String arg1) {
		// TODO Auto-generated method stub
		System.out.println("inside visitSource : "+arg0);
		
	}

	
}