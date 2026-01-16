package uci.ics.mondego.tldr.extractor;

import org.objectweb.asm.signature.SignatureVisitor;

public class SignatureVisitorImpl implements SignatureVisitor{

	public SignatureVisitor visitArrayType() {
		// TODO Auto-generated method stub
		return null;
	}

	public void visitBaseType(char arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0+"   here");
	}

	public SignatureVisitor visitClassBound() {
		// TODO Auto-generated method stub
		return null;
	}

	public void visitClassType(String arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0+"   visitClassType");
	}

	public void visitEnd() {
		// TODO Auto-generated method stub
		
	}

	public SignatureVisitor visitExceptionType() {
		// TODO Auto-generated method stub
		return null;
	}

	public void visitFormalTypeParameter(String arg0) {
		// TODO Auto-generated method stub
		
		System.out.println(arg0+"   visitFormalTypeParameter");
		
	}

	public void visitInnerClassType(String arg0) {
		// TODO Auto-generated method stub
		
	}

	public SignatureVisitor visitInterface() {
		// TODO Auto-generated method stub
		return null;
	}

	public SignatureVisitor visitInterfaceBound() {
		// TODO Auto-generated method stub
		return null;
	}

	public SignatureVisitor visitParameterType() {
		// TODO Auto-generated method stub
		return null;
	}

	public SignatureVisitor visitReturnType() {
		// TODO Auto-generated method stub
		return null;
	}

	public SignatureVisitor visitSuperclass() {
		// TODO Auto-generated method stub
		return null;
	}

	public void visitTypeArgument() {
		// TODO Auto-generated method stub
		
	}

	public SignatureVisitor visitTypeArgument(char arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	public void visitTypeVariable(String arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0+"   visitTypeVariable");
		
	}
	

}