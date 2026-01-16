package uci.ics.mondego.tldr.model;

public class Operator {

	private final Integer operator;
	private final Integer operand1;
	private final Integer operand2;
	
	public Operator(Integer operator){
		this.operator = operator;
		this.operand1 = this.operand2 = -1;
	}
	
	public Operator(Integer operator, Integer operator1){
		this.operator = operator;
		this.operand1 = operator1;
		this.operand2 = -1;
	}
	
	public Operator(Integer operator, Integer operator1, Integer operator2){
		this.operator = operator;
		this.operand1 = operator1;
		this.operand2 = operator2;
	}
	
	public Integer getOperator() {
		return operator;
	}

	public Integer getOperand1() {
		return operand1;
	}

	public Integer getOperand2() {
		return operand2;
	}
}