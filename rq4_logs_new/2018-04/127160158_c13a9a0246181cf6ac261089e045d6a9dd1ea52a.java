

package com.honours.louis.politicsgame.org.pielot.rf;

import java.util.List;

public class NegativePoliticsGameRandomForest extends RandomForest {

public String tier;
public String approval;
public String budget;
public String stability;
public double situation;

public String toString() {
StringBuilder b = new StringBuilder();
b.append("MyClass: ");
b.append(MyClass);
b.append(", tier: ");
b.append(tier);
b.append(", approval: ");
b.append(approval);
b.append(", budget: ");
b.append(budget);
b.append(", stability: ");
b.append(stability);
b.append(", situation: ");
b.append(situation);
return b.toString();
}

public void updateFields(String a, String b, String s, double sit, String t){
	this.approval = a; this.budget = b; this.stability = s; this.situation = sit; this.tier = t;
}

protected void runClassifiers(List<Prediction> predictions) {
	predictions.add(runTree0());
	predictions.add(runTree1());
	predictions.add(runTree2());
	predictions.add(runTree3());
	predictions.add(runTree4());
	predictions.add(runTree5());
	predictions.add(runTree6());
	predictions.add(runTree7());
	predictions.add(runTree8());
	predictions.add(runTree9());
	predictions.add(runTree10());
	predictions.add(runTree11());
	predictions.add(runTree12());
	predictions.add(runTree13());
	predictions.add(runTree14());
	predictions.add(runTree15());
	predictions.add(runTree16());
	predictions.add(runTree17());
	predictions.add(runTree18());
	predictions.add(runTree19());
	predictions.add(runTree20());
	predictions.add(runTree21());
	predictions.add(runTree22());
	predictions.add(runTree23());
	predictions.add(runTree24());
	predictions.add(runTree25());
	predictions.add(runTree26());
	predictions.add(runTree27());
	predictions.add(runTree28());
	predictions.add(runTree29());
	predictions.add(runTree30());
	predictions.add(runTree31());
	predictions.add(runTree32());
	predictions.add(runTree33());
	predictions.add(runTree34());
	predictions.add(runTree35());
	predictions.add(runTree36());
	predictions.add(runTree37());
	predictions.add(runTree38());
	predictions.add(runTree39());
	predictions.add(runTree40());
	predictions.add(runTree41());
	predictions.add(runTree42());
	predictions.add(runTree43());
	predictions.add(runTree44());
	predictions.add(runTree45());
	predictions.add(runTree46());
	predictions.add(runTree47());
	predictions.add(runTree48());
	predictions.add(runTree49());
	predictions.add(runTree50());
	predictions.add(runTree51());
	predictions.add(runTree52());
	predictions.add(runTree53());
	predictions.add(runTree54());
	predictions.add(runTree55());
	predictions.add(runTree56());
	predictions.add(runTree57());
	predictions.add(runTree58());
	predictions.add(runTree59());
	predictions.add(runTree60());
	predictions.add(runTree61());
	predictions.add(runTree62());
	predictions.add(runTree63());
	predictions.add(runTree64());
	predictions.add(runTree65());
	predictions.add(runTree66());
	predictions.add(runTree67());
	predictions.add(runTree68());
	predictions.add(runTree69());
	predictions.add(runTree70());
	predictions.add(runTree71());
	predictions.add(runTree72());
	predictions.add(runTree73());
	predictions.add(runTree74());
	predictions.add(runTree75());
	predictions.add(runTree76());
	predictions.add(runTree77());
	predictions.add(runTree78());
	predictions.add(runTree79());
	predictions.add(runTree80());
	predictions.add(runTree81());
	predictions.add(runTree82());
	predictions.add(runTree83());
	predictions.add(runTree84());
	predictions.add(runTree85());
	predictions.add(runTree86());
	predictions.add(runTree87());
	predictions.add(runTree88());
	predictions.add(runTree89());
	predictions.add(runTree90());
	predictions.add(runTree91());
	predictions.add(runTree92());
	predictions.add(runTree93());
	predictions.add(runTree94());
	predictions.add(runTree95());
	predictions.add(runTree96());
	predictions.add(runTree97());
	predictions.add(runTree98());
	predictions.add(runTree99());
}

private Prediction runTree0() {
	if( "medium".equals(approval) ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 1, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 40.25 ) { return new Prediction("3", 9, 0); }
	if( situation < 40.25 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.92 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
	}
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
		if( situation >= 42 ) { return new Prediction("1", 3, 1); }
		if( situation < 42 ) { return new Prediction("1", 4, 0); }
	}
	if( "low".equals(stability) ) { 
		if( situation >= 35.78 ) { return new Prediction("3", 2, 0); }
	if( situation < 35.78 ) { 
		if( situation < 22.39 ) { return new Prediction("1", 3, 0); }
	if( situation >= 22.39 ) { 
		if( situation >= 27.98 ) { return new Prediction("1", 1, 0); }
		if( situation < 27.98 ) { return new Prediction("2", 2, 0); }
	}
	}
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 11, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 38.6 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 4, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 2, 0); }
	}
	if( situation < 38.6 ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 4, 0); }
	if( "low".equals(stability) ) { 
		if( situation < 31.4 ) { return new Prediction("2", 2, 0); }
		if( situation >= 31.4 ) { return new Prediction("1", 1, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "one".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 1, 0); }
	if( "medium".equals(budget) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 2, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 2, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 4, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 4, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
return null;
}
private Prediction runTree1() {
	if( "medium".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 2, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 42 ) { return new Prediction("3", 3, 1); }
		if( situation < 42 ) { return new Prediction("1", 4, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 31 ) { return new Prediction("1", 1, 0); }
	if( situation >= 31 ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
	if( "low".equals(approval) ) { 
	if( situation < 42.38 ) { 
		if( situation < 30.59 ) { return new Prediction("2", 3, 0); }
	if( situation >= 30.59 ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 3, 0); }
	}
	}
		if( situation >= 42.38 ) { return new Prediction("1", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.39 ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 5, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 9, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 2, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 2, 0); }
	}
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 42.35 ) { return new Prediction("2", 4, 2); }
		if( situation >= 42.35 ) { return new Prediction("3", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 6, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
return null;
}
private Prediction runTree2() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 42.35 ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 2, 1); }
		if( situation < 33.92 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( situation >= 42.35 ) { return new Prediction("3", 3, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 3, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 41.4 ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 6, 0); }
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.39 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 2, 0); }
	if( situation >= 28.32 ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.98 ) { return new Prediction("1", 2, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 4, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 34.92 ) { 
		if( situation < 28.1 ) { return new Prediction("2", 3, 0); }
	if( situation >= 28.1 ) { 
		if( situation >= 33.27 ) { return new Prediction("2", 3, 0); }
		if( situation < 33.27 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( situation >= 34.92 ) { return new Prediction("1", 5, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 41.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 2, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
	if( "medium".equals(budget) ) { 
		if( situation < 44.4 ) { return new Prediction("3", 2, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 2, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "medium".equals(stability) ) { 
	if( situation >= 46.2 ) { 
		if( situation >= 48.6 ) { return new Prediction("2", 3, 0); }
		if( situation < 48.6 ) { return new Prediction("1", 3, 0); }
	}
		if( situation < 46.2 ) { return new Prediction("2", 7, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 5, 0); }
	}
return null;
}
private Prediction runTree3() {
	if( "medium".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 2, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 39 ) { return new Prediction("1", 2, 0); }
		if( situation >= 39 ) { return new Prediction("2", 3, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
	if( "low".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("2", 4, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 38.84 ) { return new Prediction("2", 1, 0); }
		if( situation < 38.84 ) { return new Prediction("3", 3, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 33.98 ) { 
	if( situation < 44.4 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 1, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 2, 1); }
	}
	if( situation >= 44.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 1, 0); }
	}
	}
	if( situation < 33.98 ) { 
		if( situation >= 27.98 ) { return new Prediction("1", 2, 0); }
	if( situation < 27.98 ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 2, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 2, 0); }
	if( situation >= 28.1 ) { 
	if( situation >= 33.27 ) { 
		if( situation < 34.92 ) { return new Prediction("1", 2, 1); }
		if( situation >= 34.92 ) { return new Prediction("1", 3, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 3, 0); }
	}
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 1, 0); }
	}
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "high".equals(stability) ) { return new Prediction("3", 23, 0); }
return null;
}
private Prediction runTree4() {
	if( "medium".equals(approval) ) { 
	if( "medium".equals(stability) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 3, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("2", 6, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 33.27 ) { return new Prediction("3", 10, 0); }
		if( situation < 33.27 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "high".equals(approval) ) { 
	if( situation < 39.35 ) { 
	if( situation < 34.92 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 6, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 2, 0); }
	if( situation >= 22.39 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 33.27 ) { return new Prediction("2", 6, 2); }
		if( situation < 33.27 ) { return new Prediction("1", 2, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
		if( situation >= 34.92 ) { return new Prediction("1", 10, 0); }
	}
	if( situation >= 39.35 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 3, 0); }
	if( situation >= 41.4 ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 3, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 1, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "medium".equals(stability) ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 5, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "one".equals(tier) ) { 
		if( situation < 32.61 ) { return new Prediction("3", 1, 0); }
		if( situation >= 32.61 ) { return new Prediction("2", 2, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 3, 0); }
	}
return null;
}
private Prediction runTree5() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 3, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 11, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 3, 1); }
	if( situation < 42.6 ) { 
		if( situation < 39 ) { return new Prediction("1", 2, 0); }
		if( situation >= 39 ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "low".equals(stability) ) { 
		if( situation < 28.32 ) { return new Prediction("2", 4, 0); }
	if( situation >= 28.32 ) { 
	if( situation >= 35.78 ) { 
		if( situation < 44.4 ) { return new Prediction("3", 2, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 35.78 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 7, 0); }
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 2, 0); }
	}
	if( situation < 43.8 ) { 
		if( situation < 31 ) { return new Prediction("1", 1, 0); }
		if( situation >= 31 ) { return new Prediction("3", 1, 0); }
	}
	}
	if( "medium".equals(budget) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 4, 0); }
	}
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 2, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 7, 0); }
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "five".equals(tier) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 2, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 3, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 3, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 6, 0); }
	}
return null;
}
private Prediction runTree6() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 28.55 ) { return new Prediction("3", 1, 0); }
	if( situation >= 28.55 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.92 ) { return new Prediction("2", 4, 1); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
	if( "high".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 39 ) { return new Prediction("1", 10, 0); }
	if( situation >= 39 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "medium".equals(budget) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 44.4 ) { 
		if( situation >= 30.19 ) { return new Prediction("2", 1, 0); }
		if( situation < 30.19 ) { return new Prediction("1", 1, 0); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 34.92 ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
	if( situation >= 28.1 ) { 
		if( situation < 31.63 ) { return new Prediction("1", 1, 0); }
		if( situation >= 31.63 ) { return new Prediction("2", 3, 1); }
	}
	}
		if( situation >= 34.92 ) { return new Prediction("1", 4, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 3, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 3, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 4, 0); }
	}
	if( "five".equals(tier) ) { 
	if( situation < 41.78 ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 2, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 2, 0); }
	}
		if( situation >= 41.78 ) { return new Prediction("2", 1, 0); }
	}
	if( "two".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("2", 2, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 2, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 3, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
return null;
}
private Prediction runTree7() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 1, 0); }
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 39 ) { return new Prediction("1", 3, 0); }
	if( situation >= 39 ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 36.23 ) { 
		if( situation < 45 ) { return new Prediction("3", 2, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 36.23 ) { return new Prediction("1", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation < 32.61 ) { return new Prediction("3", 3, 0); }
		if( situation >= 32.61 ) { return new Prediction("2", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
		if( situation < 28.32 ) { return new Prediction("2", 4, 0); }
	if( situation >= 28.32 ) { 
	if( situation >= 41.15 ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 2, 0); }
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 44.4 ) { return new Prediction("3", 2, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	if( situation < 41.15 ) { 
	if( situation >= 33.27 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 34.92 ) { return new Prediction("1", 2, 1); }
		if( situation >= 34.92 ) { return new Prediction("1", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 5, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 8, 0); }
	}
	}
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 33.92 ) { 
		if( situation < 42.35 ) { return new Prediction("3", 5, 2); }
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 33.92 ) { return new Prediction("3", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 7, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
return null;
}
private Prediction runTree8() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
	if( "low".equals(budget) ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("2", 4, 2); }
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
	if( "four".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 2, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 3, 0); }
	}
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 39 ) { return new Prediction("1", 2, 0); }
	if( situation >= 39 ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 2, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "medium".equals(budget) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.39 ) { 
		if( situation < 36.6 ) { return new Prediction("2", 1, 0); }
		if( situation >= 36.6 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 4, 0); }
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
	if( situation >= 36.23 ) { 
		if( situation < 45 ) { return new Prediction("3", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 36.23 ) { return new Prediction("1", 6, 0); }
	}
	if( "medium".equals(budget) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 10, 0); }
	}
	}
	if( "one".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 1, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 2, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 2, 0); }
	if( situation >= 39.95 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( situation < 35.42 ) { return new Prediction("2", 6, 0); }
	if( situation >= 35.42 ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 4, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 2, 0); }
	}
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 7, 0); }
	}
return null;
}
private Prediction runTree9() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 16, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 39 ) { return new Prediction("1", 4, 0); }
	if( situation >= 39 ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
	if( situation < 43.8 ) { 
		if( situation >= 36.23 ) { return new Prediction("3", 3, 0); }
		if( situation < 36.23 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 4, 0); }
	}
	if( "medium".equals(budget) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 33.98 ) { 
		if( situation < 44.4 ) { return new Prediction("2", 2, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 2, 0); }
	}
		if( situation < 33.98 ) { return new Prediction("1", 4, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
	if( situation >= 28.1 ) { 
	if( situation >= 33.27 ) { 
		if( situation < 34.92 ) { return new Prediction("2", 2, 0); }
		if( situation >= 34.92 ) { return new Prediction("1", 2, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 4, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 2, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 41 ) { return new Prediction("1", 1, 0); }
		if( situation < 41 ) { return new Prediction("2", 6, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 6, 0); }
	}
return null;
}
private Prediction runTree10() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 40.2 ) { return new Prediction("1", 6, 0); }
	if( situation >= 40.2 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 3, 0); }
		if( situation < 46.8 ) { return new Prediction("3", 3, 0); }
	}
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 2, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 1, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 38.84 ) { return new Prediction("2", 1, 0); }
		if( situation < 38.84 ) { return new Prediction("3", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 4, 0); }
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 41.4 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 6, 0); }
	if( situation >= 28.32 ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 3, 0); }
		if( situation < 33.98 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( situation >= 41.4 ) { return new Prediction("2", 2, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 6, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 4, 0); }
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 3, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 3, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(stability) ) { 
		if( situation >= 40.25 ) { return new Prediction("3", 8, 0); }
	if( situation < 40.25 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 6, 3); }
		if( situation < 33.92 ) { return new Prediction("2", 3, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
	}
return null;
}
private Prediction runTree11() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.92 ) { return new Prediction("3", 3, 1); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 3, 0); }
	}
	if( "high".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 40.8 ) { return new Prediction("1", 2, 0); }
		if( situation >= 40.8 ) { return new Prediction("3", 3, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("3", 2, 0); }
		if( situation < 36.23 ) { return new Prediction("1", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	if( "medium".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 35.78 ) { return new Prediction("3", 1, 0); }
	if( situation < 35.78 ) { 
		if( situation < 22.39 ) { return new Prediction("1", 2, 0); }
	if( situation >= 22.39 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 5, 0); }
		if( situation >= 28.32 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 34.92 ) { 
		if( situation < 28.1 ) { return new Prediction("2", 2, 0); }
	if( situation >= 28.1 ) { 
		if( situation < 31.63 ) { return new Prediction("1", 2, 0); }
		if( situation >= 31.63 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( situation >= 34.92 ) { return new Prediction("1", 5, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 2, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( situation < 42.3 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "five".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 2, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 8, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
	if( situation >= 42.3 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 3, 0); }
		if( situation >= 45 ) { return new Prediction("1", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("2", 5, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
return null;
}
private Prediction runTree12() {
	if( "medium".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( "high".equals(budget) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
	if( "low".equals(budget) ) { 
	if( situation < 40.8 ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 1, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 1, 0); }
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
		if( situation >= 40.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "five".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 2, 0); }
		if( "low".equals(approval) ) { return new Prediction("2", 4, 0); }
	}
	if( "two".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(approval) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 2, 0); }
	}
	if( situation < 43.8 ) { 
		if( situation >= 36.23 ) { return new Prediction("3", 3, 0); }
		if( situation < 36.23 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("2", 1, 0); }
	}
	if( "one".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 1, 0); }
	if( "low".equals(approval) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 4, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 1, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 2, 0); }
		if( "low".equals(approval) ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
		if( situation >= 42 ) { return new Prediction("2", 5, 2); }
	if( situation < 42 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 3, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 3, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 10, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 3, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "high".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 2, 1); }
		if( situation < 33.92 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "high".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(approval) ) { return new Prediction("3", 12, 0); }
	}
return null;
}
private Prediction runTree13() {
	if( "high".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 5, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 3, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 4, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 3, 0); }
	}
	if( "low".equals(budget) ) { 
	if( "medium".equals(approval) ) { 
		if( situation >= 40.25 ) { return new Prediction("3", 6, 0); }
	if( situation < 40.25 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 33.73 ) { return new Prediction("3", 1, 0); }
		if( situation >= 33.73 ) { return new Prediction("2", 2, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 3, 0); }
	}
	}
	if( "high".equals(approval) ) { 
		if( situation < 40.2 ) { return new Prediction("1", 3, 0); }
	if( situation >= 40.2 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
	if( situation < 46.8 ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
	}
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	if( "medium".equals(budget) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 4, 0); }
	if( "high".equals(approval) ) { 
	if( situation >= 42 ) { 
		if( situation < 44.4 ) { return new Prediction("3", 3, 1); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	if( situation < 42 ) { 
	if( situation >= 27.98 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.98 ) { return new Prediction("1", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 3, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 2, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( situation < 27.98 ) { 
		if( situation >= 23.3 ) { return new Prediction("2", 2, 0); }
		if( situation < 23.3 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
		if( "low".equals(approval) ) { return new Prediction("3", 11, 0); }
	}
return null;
}
private Prediction runTree14() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 40.2 ) { return new Prediction("1", 9, 0); }
	if( situation >= 40.2 ) { 
	if( situation >= 42.6 ) { 
		if( "three".equals(tier) ) { return new Prediction("3", 4, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
		if( situation < 42.6 ) { return new Prediction("3", 4, 0); }
	}
	}
	if( "low".equals(approval) ) { 
		if( situation >= 46.2 ) { return new Prediction("1", 2, 0); }
	if( situation < 46.2 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 2, 0); }
	if( situation >= 30.59 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 2, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 41.4 ) { 
		if( situation < 22.73 ) { return new Prediction("1", 3, 0); }
	if( situation >= 22.73 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 2, 0); }
	if( situation >= 28.32 ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 2, 0); }
		if( situation < 33.98 ) { return new Prediction("1", 2, 0); }
	}
	}
	}
		if( situation >= 41.4 ) { return new Prediction("3", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 34.92 ) { 
		if( situation >= 33.27 ) { return new Prediction("1", 2, 1); }
		if( situation < 33.27 ) { return new Prediction("1", 1, 0); }
	}
		if( situation >= 34.92 ) { return new Prediction("1", 5, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 3, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 4, 0); }
	}
	if( "high".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
		if( situation >= 33.92 ) { return new Prediction("3", 7, 0); }
		if( situation < 33.92 ) { return new Prediction("2", 1, 0); }
	}
		if( "high".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(approval) ) { return new Prediction("3", 7, 0); }
	}
return null;
}
private Prediction runTree15() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 28.55 ) { return new Prediction("3", 1, 0); }
		if( situation >= 28.55 ) { return new Prediction("3", 3, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 4, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 41.4 ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 2, 0); }
	if( "medium".equals(budget) ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.98 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
	if( situation >= 28.1 ) { 
		if( situation < 32.55 ) { return new Prediction("1", 4, 0); }
	if( situation >= 32.55 ) { 
		if( situation < 34.92 ) { return new Prediction("2", 2, 0); }
		if( situation >= 34.92 ) { return new Prediction("1", 3, 0); }
	}
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 7, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	if( situation >= 41.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("3", 5, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	if( "medium".equals(budget) ) { 
		if( situation < 44.4 ) { return new Prediction("2", 1, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "medium".equals(stability) ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 3, 0); }
	if( situation >= 30.59 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 2, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 2, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation < 32.61 ) { return new Prediction("3", 1, 0); }
		if( situation >= 32.61 ) { return new Prediction("2", 3, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 7, 0); }
	}
return null;
}
private Prediction runTree16() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 28.55 ) { return new Prediction("3", 3, 0); }
		if( situation >= 28.55 ) { return new Prediction("2", 4, 2); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 39 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 8, 0); }
	if( "medium".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 2, 0); }
	if( situation >= 22.39 ) { 
		if( situation >= 27.98 ) { return new Prediction("1", 1, 0); }
		if( situation < 27.98 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
	if( situation >= 28.1 ) { 
	if( situation >= 33.27 ) { 
		if( situation < 34.92 ) { return new Prediction("1", 2, 1); }
		if( situation >= 34.92 ) { return new Prediction("1", 2, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 3, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 7, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( situation >= 39 ) { 
	if( situation >= 42.6 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 44.4 ) { return new Prediction("2", 3, 1); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
	if( situation < 46.8 ) { 
		if( "medium".equals(stability) ) { return new Prediction("2", 3, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 1, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	if( situation < 42.6 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 4, 0); }
		if( situation >= 41.4 ) { return new Prediction("3", 2, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "medium".equals(stability) ) { 
		if( situation >= 46.2 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.2 ) { return new Prediction("2", 5, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 7, 0); }
	}
return null;
}
private Prediction runTree17() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 42.35 ) { return new Prediction("2", 5, 0); }
	if( situation >= 42.35 ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 2, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 2, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "high".equals(approval) ) { 
	if( situation < 40.8 ) { 
	if( situation < 39.35 ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 9, 0); }
	if( "low".equals(stability) ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 4, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 29.75 ) { return new Prediction("2", 1, 0); }
	if( situation >= 29.75 ) { 
	if( situation >= 33.27 ) { 
		if( situation < 34.92 ) { return new Prediction("1", 2, 1); }
		if( situation >= 34.92 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( situation >= 39.35 ) { return new Prediction("2", 4, 0); }
	}
	if( situation >= 40.8 ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 44.4 ) { return new Prediction("3", 2, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 6, 0); }
	if( situation < 36.8 ) { 
	if( situation < 35.42 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 6, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( situation >= 35.42 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 7, 0); }
	}
return null;
}
private Prediction runTree18() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 1, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 37.8 ) { return new Prediction("1", 6, 0); }
	if( situation >= 37.8 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 4, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 2, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation < 32.61 ) { return new Prediction("3", 3, 0); }
		if( situation >= 32.61 ) { return new Prediction("2", 2, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 2, 0); }
	if( situation >= 22.39 ) { 
	if( situation < 44.4 ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 4, 0); }
	if( situation < 33.98 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 3, 0); }
		if( situation >= 28.32 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 4, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 42.35 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 2, 1); }
		if( situation < 33.92 ) { return new Prediction("3", 3, 1); }
	}
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 7, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 4, 0); }
	}
return null;
}
private Prediction runTree19() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 25.18 ) { return new Prediction("2", 1, 0); }
	if( situation >= 25.18 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.92 ) { return new Prediction("3", 5, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 3, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 39 ) { 
		if( situation >= 27.98 ) { return new Prediction("1", 6, 0); }
	if( situation < 27.98 ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( situation >= 39 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
	if( "medium".equals(budget) ) { 
		if( situation < 44.4 ) { return new Prediction("2", 4, 1); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 11, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 2, 0); }
	if( situation >= 28.1 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
	if( situation < 45 ) { 
		if( situation >= 36.23 ) { return new Prediction("3", 1, 0); }
		if( situation < 36.23 ) { return new Prediction("1", 1, 0); }
	}
		if( situation >= 45 ) { return new Prediction("1", 2, 0); }
	}
	if( "medium".equals(budget) ) { 
	if( situation < 34.92 ) { 
		if( situation >= 33.27 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.27 ) { return new Prediction("1", 2, 0); }
	}
		if( situation >= 34.92 ) { return new Prediction("1", 6, 0); }
	}
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 4, 0); }
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
	if( "two".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 1, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 43.02 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.02 ) { return new Prediction("3", 3, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
return null;
}
private Prediction runTree20() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 1, 0); }
	if( "high".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 36.23 ) { 
		if( situation < 45 ) { return new Prediction("3", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 36.23 ) { return new Prediction("1", 4, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
	if( "low".equals(approval) ) { 
	if( situation >= 36.8 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 3, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
	if( situation < 36.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 5, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 1, 0); }
	if( "high".equals(approval) ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 2, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 2, 0); }
	if( situation >= 28.1 ) { 
	if( situation >= 33.27 ) { 
		if( situation < 35.15 ) { return new Prediction("2", 2, 0); }
		if( situation >= 35.15 ) { return new Prediction("1", 2, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 3, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 4, 0); }
	}
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 42.35 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 2, 1); }
		if( situation < 33.92 ) { return new Prediction("3", 4, 1); }
	}
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
return null;
}
private Prediction runTree21() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 2, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
	if( situation < 42.35 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 2, 1); }
		if( situation < 33.92 ) { return new Prediction("3", 1, 0); }
	}
		if( situation >= 42.35 ) { return new Prediction("3", 3, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 39.35 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.39 ) { 
		if( situation >= 27.98 ) { return new Prediction("1", 1, 0); }
		if( situation < 27.98 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 29.02 ) { return new Prediction("1", 5, 0); }
		if( situation < 29.02 ) { return new Prediction("2", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 5, 0); }
	}
	if( situation >= 39.35 ) { 
	if( situation < 44.4 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 6, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	}
	if( situation >= 44.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
	if( situation < 46.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 5, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( situation >= 36.8 ) { 
	if( "medium".equals(stability) ) { 
		if( situation >= 46.2 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.2 ) { return new Prediction("2", 8, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 1, 0); }
	}
	if( situation < 36.8 ) { 
	if( situation >= 29.34 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "five".equals(tier) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( situation < 29.34 ) { return new Prediction("2", 1, 0); }
	}
	}
return null;
}
private Prediction runTree22() {
	if( "medium".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(approval) ) { 
		if( situation < 39 ) { return new Prediction("1", 8, 0); }
	if( situation >= 39 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 4, 1); }
		if( situation < 42.6 ) { return new Prediction("2", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 5, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 43.02 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.02 ) { return new Prediction("3", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( situation >= 23.3 ) { 
	if( situation >= 27.98 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 35.78 ) { return new Prediction("2", 1, 0); }
		if( situation < 35.78 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 7, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 3, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 2, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( situation < 27.98 ) { return new Prediction("2", 2, 0); }
	}
		if( situation < 23.3 ) { return new Prediction("1", 5, 0); }
	}
	if( "high".equals(stability) ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 2, 1); }
		if( "five".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
return null;
}
private Prediction runTree23() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
	if( situation >= 33.92 ) { 
		if( situation < 42.35 ) { return new Prediction("2", 4, 0); }
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 33.92 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
	if( "four".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 3, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 1, 0); }
	}
	}
	if( "high".equals(approval) ) { 
	if( situation < 41.4 ) { 
	if( situation >= 26.43 ) { 
	if( "medium".equals(stability) ) { 
		if( situation < 39 ) { return new Prediction("1", 5, 0); }
		if( situation >= 39 ) { return new Prediction("2", 1, 0); }
	}
	if( "low".equals(stability) ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 33.27 ) { 
		if( situation < 34.92 ) { return new Prediction("2", 1, 0); }
		if( situation >= 34.92 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 1, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 5, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 2, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 3, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( situation < 26.43 ) { return new Prediction("1", 4, 0); }
	}
	if( situation >= 41.4 ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 3, 1); }
	if( "medium".equals(budget) ) { 
		if( situation < 44.4 ) { return new Prediction("2", 2, 1); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
	if( "medium".equals(stability) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 1, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 4, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 3, 0); }
	if( situation >= 30.59 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 3, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 2, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 5, 0); }
	}
return null;
}
private Prediction runTree24() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 4, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 38.45 ) { return new Prediction("3", 5, 0); }
	if( situation >= 38.45 ) { 
		if( situation >= 40.25 ) { return new Prediction("3", 4, 0); }
		if( situation < 40.25 ) { return new Prediction("2", 2, 1); }
	}
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 6, 0); }
	if( "low".equals(stability) ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 3, 0); }
	if( situation < 42.6 ) { 
		if( situation < 22.39 ) { return new Prediction("1", 2, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 4, 0); }
	}
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 2, 0); }
	}
	if( situation < 43.8 ) { 
		if( situation >= 36.23 ) { return new Prediction("3", 1, 0); }
		if( situation < 36.23 ) { return new Prediction("1", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( situation >= 33.27 ) { 
		if( situation >= 40.13 ) { return new Prediction("1", 1, 0); }
		if( situation < 40.13 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 2, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 7, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 6, 0); }
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
	if( situation >= 36.8 ) { 
	if( situation >= 46.2 ) { 
		if( situation >= 48.6 ) { return new Prediction("2", 1, 0); }
		if( situation < 48.6 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 46.2 ) { return new Prediction("2", 7, 0); }
	}
	if( situation < 36.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 1, 0); }
	if( situation >= 30.59 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 5, 0); }
	}
return null;
}
private Prediction runTree25() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 1, 0); }
	if( "high".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 4, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 36.23 ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 2, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 3, 0); }
	}
		if( situation < 36.23 ) { return new Prediction("1", 3, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 4, 0); }
	}
	if( "low".equals(approval) ) { 
	if( situation < 34.92 ) { 
		if( situation < 31.17 ) { return new Prediction("2", 3, 0); }
		if( situation >= 31.17 ) { return new Prediction("3", 2, 0); }
	}
		if( situation >= 34.92 ) { return new Prediction("2", 5, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "medium".equals(budget) ) { 
	if( situation >= 33.98 ) { 
	if( situation < 44.4 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 2, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 4, 2); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 33.98 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 33.27 ) { 
		if( situation < 34.92 ) { return new Prediction("2", 3, 1); }
		if( situation >= 34.92 ) { return new Prediction("1", 3, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 3, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 2, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(stability) ) { 
		if( situation >= 31.98 ) { return new Prediction("3", 16, 0); }
	if( situation < 31.98 ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 2, 1); }
		if( "high".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(approval) ) { return new Prediction("3", 2, 0); }
	}
	}
return null;
}
private Prediction runTree26() {
	if( "high".equals(budget) ) { 
	if( situation < 39.42 ) { 
	if( situation < 35.42 ) { 
		if( situation >= 28.28 ) { return new Prediction("2", 4, 0); }
		if( situation < 28.28 ) { return new Prediction("3", 2, 0); }
	}
		if( situation >= 35.42 ) { return new Prediction("3", 3, 0); }
	}
	if( situation >= 39.42 ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 2, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(approval) ) { 
		if( situation >= 46.2 ) { return new Prediction("1", 2, 0); }
		if( situation < 46.2 ) { return new Prediction("2", 2, 0); }
	}
	}
	}
	if( "low".equals(budget) ) { 
	if( "medium".equals(approval) ) { 
	if( situation >= 31.98 ) { 
		if( situation >= 40.25 ) { return new Prediction("3", 9, 0); }
	if( situation < 40.25 ) { 
		if( situation < 38.45 ) { return new Prediction("3", 3, 0); }
	if( situation >= 38.45 ) { 
		if( "three".equals(tier) ) { return new Prediction("3", 4, 1); }
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
		if( situation < 31.98 ) { return new Prediction("2", 3, 1); }
	}
	if( "high".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 36.23 ) { 
		if( situation < 45 ) { return new Prediction("3", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 36.23 ) { return new Prediction("1", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	if( "medium".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 28.32 ) { return new Prediction("2", 6, 0); }
	if( situation >= 28.32 ) { 
	if( situation >= 33.98 ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 33.98 ) { return new Prediction("1", 1, 0); }
	}
	}
	if( "five".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 6, 0); }
		if( "low".equals(approval) ) { return new Prediction("3", 3, 0); }
	}
	if( "two".equals(tier) ) { 
	if( situation < 34.92 ) { 
		if( situation >= 33.27 ) { return new Prediction("2", 3, 0); }
	if( situation < 33.27 ) { 
		if( situation < 28.1 ) { return new Prediction("2", 2, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( situation >= 34.92 ) { return new Prediction("1", 4, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 5, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
return null;
}
private Prediction runTree27() {
	if( "medium".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( "high".equals(budget) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 2, 0); }
	}
	if( "low".equals(budget) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 1, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 1, 0); }
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 2, 0); }
	}
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 2, 0); }
	if( situation >= 30.59 ) { 
		if( situation < 38.75 ) { return new Prediction("3", 3, 0); }
		if( situation >= 38.75 ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "two".equals(tier) ) { 
	if( situation >= 36.23 ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 3, 0); }
	}
	if( situation < 36.23 ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 1, 0); }
		if( "low".equals(approval) ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "one".equals(tier) ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 3, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	if( "low".equals(stability) ) { 
		if( situation < 28.99 ) { return new Prediction("2", 4, 0); }
	if( situation >= 28.99 ) { 
	if( "three".equals(tier) ) { 
	if( situation < 44.4 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 2, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 2, 1); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 35.15 ) { 
		if( situation >= 33.27 ) { return new Prediction("1", 2, 1); }
		if( situation < 33.27 ) { return new Prediction("1", 1, 0); }
	}
		if( situation >= 35.15 ) { return new Prediction("1", 2, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 3, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 2, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 3, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	if( "high".equals(stability) ) { 
	if( situation >= 31.18 ) { 
		if( situation >= 40.25 ) { return new Prediction("3", 7, 0); }
	if( situation < 40.25 ) { 
		if( "three".equals(tier) ) { return new Prediction("3", 4, 1); }
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
	}
		if( situation < 31.18 ) { return new Prediction("2", 3, 1); }
	}
return null;
}
private Prediction runTree28() {
	if( "medium".equals(stability) ) { 
	if( "high".equals(budget) ) { 
	if( situation >= 46.2 ) { 
		if( situation >= 48.6 ) { return new Prediction("2", 1, 0); }
		if( situation < 48.6 ) { return new Prediction("1", 1, 0); }
	}
	if( situation < 46.2 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 3, 0); }
	if( situation >= 30.59 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 3, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("2", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 2, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39 ) { return new Prediction("2", 2, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 3, 0); }
	}
	if( situation < 43.8 ) { 
		if( situation >= 36.23 ) { return new Prediction("3", 3, 0); }
		if( situation < 36.23 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 41.4 ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.98 ) { return new Prediction("1", 2, 0); }
	}
	if( situation >= 41.4 ) { 
		if( situation < 44.4 ) { return new Prediction("3", 1, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 4, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.92 ) { return new Prediction("3", 4, 0); }
		if( situation < 33.92 ) { return new Prediction("3", 3, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 6, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
return null;
}
private Prediction runTree29() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 1, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 39 ) { return new Prediction("1", 10, 0); }
	if( situation >= 39 ) { 
	if( situation >= 43.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 2, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( situation < 43.8 ) { 
		if( situation < 40.8 ) { return new Prediction("2", 1, 0); }
		if( situation >= 40.8 ) { return new Prediction("3", 2, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 4, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 1, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 3, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 3, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "medium".equals(budget) ) { 
	if( situation < 41.4 ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 2, 0); }
	if( situation < 33.98 ) { 
		if( situation < 22.73 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.73 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.32 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
	if( situation >= 41.4 ) { 
		if( situation < 44.4 ) { return new Prediction("3", 2, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
	if( situation >= 28.1 ) { 
	if( situation < 35.15 ) { 
		if( situation < 31.63 ) { return new Prediction("1", 2, 0); }
		if( situation >= 31.63 ) { return new Prediction("2", 1, 0); }
	}
		if( situation >= 35.15 ) { return new Prediction("1", 3, 0); }
	}
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(stability) ) { 
		if( situation < 29.07 ) { return new Prediction("2", 3, 1); }
		if( situation >= 29.07 ) { return new Prediction("3", 13, 0); }
	}
return null;
}
private Prediction runTree30() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 3, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("2", 1, 0); }
		if( "high".equals(stability) ) { return new Prediction("2", 4, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 3, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 2, 0); }
	if( situation >= 22.39 ) { 
	if( situation < 44.4 ) { 
	if( situation >= 33.98 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 1, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 4, 1); }
	}
	if( situation < 33.98 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 2, 0); }
		if( situation >= 28.32 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 4, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 33.27 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 3, 0); }
	if( situation < 46.8 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 3, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
	if( "medium".equals(budget) ) { 
		if( situation < 34.92 ) { return new Prediction("2", 1, 0); }
		if( situation >= 34.92 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
		if( situation < 33.27 ) { return new Prediction("1", 3, 0); }
	}
	if( "one".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 2, 0); }
	if( "low".equals(stability) ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 1, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 2, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 4, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 2, 0); }
	}
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 3, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 2, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation < 32.61 ) { return new Prediction("3", 1, 0); }
		if( situation >= 32.61 ) { return new Prediction("2", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 6, 0); }
	}
return null;
}
private Prediction runTree31() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
	if( "low".equals(budget) ) { 
	if( situation < 42.35 ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 4, 2); }
		if( situation < 33.92 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( situation >= 42.35 ) { return new Prediction("3", 3, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
	if( "high".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
	if( situation < 40.8 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 37.8 ) { return new Prediction("1", 2, 0); }
		if( situation >= 37.8 ) { return new Prediction("2", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 40.8 ) { 
	if( situation >= 42.6 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 2, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 42.6 ) { return new Prediction("3", 1, 0); }
	}
	}
	if( "medium".equals(budget) ) { 
		if( situation >= 42 ) { return new Prediction("2", 6, 2); }
	if( situation < 42 ) { 
	if( situation < 39.35 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 29.75 ) { return new Prediction("2", 1, 0); }
		if( situation >= 29.75 ) { return new Prediction("1", 8, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 39.35 ) { 
		if( situation < 40.2 ) { return new Prediction("2", 1, 0); }
	if( situation >= 40.2 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	}
	}
	}
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "five".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 4, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 3, 0); }
	}
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 3, 0); }
	}
	if( "one".equals(tier) ) { 
	if( "high".equals(budget) ) { 
		if( situation < 32.61 ) { return new Prediction("3", 1, 0); }
		if( situation >= 32.61 ) { return new Prediction("2", 5, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
return null;
}
private Prediction runTree32() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 9, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 41.4 ) { 
	if( situation < 39 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 3, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.39 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 2, 0); }
		if( situation >= 28.32 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
		if( situation >= 39 ) { return new Prediction("2", 3, 0); }
	}
	if( situation >= 41.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 5, 2); }
		if( "medium".equals(budget) ) { return new Prediction("3", 3, 1); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 33.27 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 3, 0); }
	}
	if( "medium".equals(budget) ) { 
		if( situation >= 40.13 ) { return new Prediction("1", 2, 0); }
		if( situation < 40.13 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( situation < 33.27 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 4, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 2, 0); }
	}
	}
	}
	if( "one".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 1, 0); }
	if( "medium".equals(budget) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 2, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 2, 0); }
	if( "five".equals(tier) ) { 
	if( "high".equals(budget) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 3, 0); }
	if( situation >= 30.59 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 3, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("3", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 9, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
return null;
}
private Prediction runTree33() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
		if( situation < 25.18 ) { return new Prediction("2", 3, 0); }
		if( situation >= 25.18 ) { return new Prediction("3", 1, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("2", 1, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 33.92 ) { return new Prediction("3", 1, 0); }
		if( situation < 33.92 ) { return new Prediction("2", 2, 1); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 41.4 ) { 
	if( "medium".equals(stability) ) { 
		if( situation < 37.8 ) { return new Prediction("1", 1, 0); }
		if( situation >= 37.8 ) { return new Prediction("2", 3, 0); }
	}
	if( "low".equals(stability) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.39 ) { 
	if( situation >= 27.98 ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.98 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 27.98 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 41.4 ) { 
		if( situation < 44.4 ) { return new Prediction("3", 3, 1); }
		if( situation >= 44.4 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 38.33 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 6, 0); }
	if( "medium".equals(budget) ) { 
		if( situation >= 31.13 ) { return new Prediction("1", 1, 0); }
		if( situation < 31.13 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( situation >= 38.33 ) { 
	if( situation >= 43.8 ) { 
		if( "medium".equals(stability) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 1, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 4, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 4, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 2, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( situation >= 46.2 ) { 
		if( situation >= 48.6 ) { return new Prediction("2", 1, 0); }
		if( situation < 48.6 ) { return new Prediction("1", 2, 0); }
	}
	if( situation < 46.2 ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 4, 0); }
	if( situation < 36.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 1, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 1, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 4, 0); }
	}
	}
return null;
}
private Prediction runTree34() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 33.92 ) { return new Prediction("3", 1, 0); }
		if( situation < 33.92 ) { return new Prediction("2", 2, 1); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 3, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
		if( situation < 39 ) { return new Prediction("1", 2, 0); }
	if( situation >= 39 ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 2, 1); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.39 ) { 
	if( situation < 44.4 ) { 
	if( situation >= 33.98 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 1, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 2, 1); }
	}
	if( situation < 33.98 ) { 
		if( situation >= 27.98 ) { return new Prediction("1", 1, 0); }
		if( situation < 27.98 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 9, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 33.27 ) { 
	if( "medium".equals(stability) ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 3, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
	if( "low".equals(stability) ) { 
		if( situation >= 40.13 ) { return new Prediction("1", 1, 0); }
		if( situation < 40.13 ) { return new Prediction("2", 1, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( situation < 33.27 ) { 
	if( situation < 28.1 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
		if( situation >= 28.1 ) { return new Prediction("1", 5, 0); }
	}
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 5, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 5, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 5, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 6, 0); }
	}
return null;
}
private Prediction runTree35() {
	if( "medium".equals(approval) ) { 
	if( situation >= 33.92 ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 2, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 4, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( situation < 33.92 ) { return new Prediction("2", 3, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 41.4 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 28.32 ) { return new Prediction("2", 1, 0); }
	if( situation >= 28.32 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 37.8 ) { return new Prediction("1", 1, 0); }
		if( situation >= 37.8 ) { return new Prediction("2", 1, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 10, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 4, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 3, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 1, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 2, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
	}
	}
	if( situation >= 41.4 ) { 
	if( situation < 44.4 ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 2, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 1, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 44.4 ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 4, 1); }
		if( "low".equals(stability) ) { return new Prediction("1", 3, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
	if( situation < 45 ) { 
		if( situation < 32.47 ) { return new Prediction("2", 5, 0); }
	if( situation >= 32.47 ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 2, 0); }
	}
	}
		if( situation >= 45 ) { return new Prediction("1", 2, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 6, 0); }
	}
return null;
}
private Prediction runTree36() {
		if( "medium".equals(approval) ) { return new Prediction("3", 12, 0); }
	if( "high".equals(approval) ) { 
	if( situation < 39.35 ) { 
	if( situation < 28.1 ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 6, 0); }
	if( "low".equals(stability) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 1, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( situation >= 28.1 ) { return new Prediction("1", 20, 0); }
	}
	if( situation >= 39.35 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("3", 3, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 3, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	if( "medium".equals(budget) ) { 
	if( situation < 44.4 ) { 
		if( situation >= 42 ) { return new Prediction("2", 4, 1); }
		if( situation < 42 ) { return new Prediction("2", 3, 0); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 1, 0); }
	if( situation >= 30.59 ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 2, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 3, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 1, 0); }
	}
	}
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 3, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 5, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
return null;
}
private Prediction runTree37() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 3, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 8, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 41.4 ) { 
	if( "three".equals(tier) ) { 
	if( situation < 39 ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 3, 0); }
	if( "low".equals(stability) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 2, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( situation >= 39 ) { return new Prediction("2", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 4, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 6, 0); }
	}
	}
	if( "one".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 1, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 2, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 1, 0); }
	}
	}
	}
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 2, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( situation >= 41.4 ) { 
		if( situation < 44.4 ) { return new Prediction("3", 2, 0); }
	if( situation >= 44.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 2, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "five".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 3, 0); }
	if( situation >= 30.59 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 2, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 4, 0); }
	}
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 5, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 4, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 3, 0); }
	}
return null;
}
private Prediction runTree38() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 40.2 ) { return new Prediction("1", 9, 0); }
	if( situation >= 40.2 ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 2, 0); }
		if( situation < 42.6 ) { return new Prediction("3", 1, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( situation < 45 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 2, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 1, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 1, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 45 ) { 
		if( situation >= 48.6 ) { return new Prediction("2", 2, 0); }
		if( situation < 48.6 ) { return new Prediction("1", 2, 0); }
	}
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 44.4 ) { 
		if( situation < 22.73 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.73 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 2, 0); }
		if( situation >= 41.4 ) { return new Prediction("3", 3, 1); }
	}
	}
		if( situation >= 44.4 ) { return new Prediction("1", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 29.75 ) { return new Prediction("2", 2, 0); }
		if( situation >= 29.75 ) { return new Prediction("1", 5, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 3, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 1, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 1, 0); }
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 42.35 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 2, 0); }
		if( situation < 33.92 ) { return new Prediction("3", 1, 0); }
	}
		if( situation >= 42.35 ) { return new Prediction("3", 4, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 6, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 6, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
return null;
}
private Prediction runTree39() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 4, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 40.2 ) { return new Prediction("1", 7, 0); }
	if( situation >= 40.2 ) { 
	if( situation >= 42.6 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 2, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
		if( situation < 42.6 ) { return new Prediction("3", 2, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( situation < 45 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 2, 0); }
	if( "five".equals(tier) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 3, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 3, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 5, 0); }
	if( "high".equals(approval) ) { 
	if( situation < 44.4 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 41.4 ) { return new Prediction("2", 2, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 3, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 34.92 ) { 
		if( situation < 31.63 ) { return new Prediction("1", 1, 0); }
		if( situation >= 31.63 ) { return new Prediction("2", 1, 0); }
	}
		if( situation >= 34.92 ) { return new Prediction("1", 3, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 2, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 4, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 33.92 ) { 
		if( situation < 42.35 ) { return new Prediction("2", 4, 2); }
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 33.92 ) { return new Prediction("2", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
return null;
}
private Prediction runTree40() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 4, 0); }
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 42.35 ) { return new Prediction("2", 5, 2); }
		if( situation >= 42.35 ) { return new Prediction("3", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 3, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 44.4 ) { 
	if( situation >= 33.98 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("2", 4, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 3, 1); }
	}
	if( situation < 33.98 ) { 
	if( situation < 28.32 ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 2, 0); }
	}
		if( situation >= 28.32 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( situation >= 44.4 ) { return new Prediction("1", 4, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
	if( situation >= 36.23 ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
		if( situation < 36.23 ) { return new Prediction("1", 6, 0); }
	}
	if( "medium".equals(budget) ) { 
		if( situation < 29.75 ) { return new Prediction("2", 2, 0); }
	if( situation >= 29.75 ) { 
	if( situation < 35.15 ) { 
		if( situation >= 33.27 ) { return new Prediction("1", 2, 1); }
		if( situation < 33.27 ) { return new Prediction("1", 1, 0); }
	}
		if( situation >= 35.15 ) { return new Prediction("1", 2, 0); }
	}
	}
	}
	if( "one".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 1, 0); }
	if( "low".equals(stability) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 3, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 2, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 2, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
return null;
}
private Prediction runTree41() {
	if( "medium".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
		if( situation < 25.18 ) { return new Prediction("2", 1, 0); }
		if( situation >= 25.18 ) { return new Prediction("3", 2, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 40.8 ) { 
		if( situation < 37.8 ) { return new Prediction("1", 4, 0); }
		if( situation >= 37.8 ) { return new Prediction("2", 1, 0); }
	}
	if( situation >= 40.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("3", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(approval) ) { 
		if( situation < 45 ) { return new Prediction("2", 13, 0); }
	if( situation >= 45 ) { 
		if( situation >= 48.6 ) { return new Prediction("2", 1, 0); }
		if( situation < 48.6 ) { return new Prediction("1", 3, 0); }
	}
	}
	}
	if( "low".equals(stability) ) { 
		if( situation >= 42 ) { return new Prediction("2", 2, 1); }
	if( situation < 42 ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 27.98 ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.98 ) { return new Prediction("1", 3, 0); }
	}
		if( situation < 27.98 ) { return new Prediction("2", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 6, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 4, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
	}
	}
	if( "high".equals(stability) ) { 
		if( situation >= 31.98 ) { return new Prediction("3", 15, 0); }
	if( situation < 31.98 ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 1, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(approval) ) { return new Prediction("3", 2, 0); }
	}
	}
return null;
}
private Prediction runTree42() {
	if( "medium".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 2, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 39 ) { return new Prediction("1", 4, 0); }
	if( situation >= 39 ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("1", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("2", 4, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 36.23 ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
	if( situation < 36.23 ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 3, 0); }
		if( "low".equals(approval) ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("2", 3, 0); }
	if( "four".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 1, 0); }
		if( "low".equals(approval) ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.39 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 5, 0); }
	if( situation >= 41.4 ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 2, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 44.4 ) { return new Prediction("2", 2, 1); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 7, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 7, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	if( "high".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.92 ) { return new Prediction("3", 3, 1); }
		if( situation < 33.92 ) { return new Prediction("3", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
		if( "high".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(approval) ) { return new Prediction("3", 7, 0); }
	}
return null;
}
private Prediction runTree43() {
	if( "high".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 38.84 ) { return new Prediction("2", 3, 0); }
		if( situation < 38.84 ) { return new Prediction("3", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 3, 0); }
	}
	if( "low".equals(budget) ) { 
	if( "medium".equals(stability) ) { 
	if( situation < 44.4 ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 1, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 3, 0); }
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("3", 1, 0); }
		if( situation < 36.23 ) { return new Prediction("1", 5, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	if( situation >= 44.4 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 4, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 42.35 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.92 ) { return new Prediction("3", 5, 1); }
	}
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "medium".equals(budget) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(approval) ) { 
	if( situation >= 42 ) { 
		if( situation < 44.4 ) { return new Prediction("2", 2, 1); }
		if( situation >= 44.4 ) { return new Prediction("1", 2, 0); }
	}
	if( situation < 42 ) { 
		if( situation >= 34.58 ) { return new Prediction("1", 13, 0); }
	if( situation < 34.58 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 27.98 ) { return new Prediction("1", 2, 0); }
	if( situation < 27.98 ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 1, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
		if( "low".equals(approval) ) { return new Prediction("3", 5, 0); }
	}
return null;
}
private Prediction runTree44() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 2, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 37.8 ) { return new Prediction("1", 6, 0); }
	if( situation >= 37.8 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 4, 1); }
		if( situation < 42.6 ) { return new Prediction("2", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( situation >= 28.86 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 2, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("2", 6, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 28.86 ) { return new Prediction("3", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.39 ) { 
	if( situation < 44.4 ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 3, 0); }
	if( situation < 33.98 ) { 
		if( situation >= 27.98 ) { return new Prediction("1", 1, 0); }
		if( situation < 27.98 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 35.15 ) { 
		if( situation >= 31.13 ) { return new Prediction("1", 2, 1); }
		if( situation < 31.13 ) { return new Prediction("2", 1, 0); }
	}
		if( situation >= 35.15 ) { return new Prediction("1", 3, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 4, 0); }
	}
	if( "high".equals(stability) ) { 
	if( situation < 29.07 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 3, 1); }
		if( "medium".equals(budget) ) { return new Prediction("3", 1, 0); }
	}
		if( situation >= 29.07 ) { return new Prediction("3", 16, 0); }
	}
return null;
}
private Prediction runTree45() {
	if( "medium".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 44.4 ) { return new Prediction("2", 7, 0); }
	if( situation >= 44.4 ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("3", 1, 0); }
		if( "low".equals(approval) ) { return new Prediction("1", 1, 0); }
	}
	}
	if( "five".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 2, 0); }
		if( "low".equals(approval) ) { return new Prediction("2", 3, 0); }
	}
	if( "two".equals(tier) ) { 
	if( situation >= 36.23 ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
	if( situation < 36.23 ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 3, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 3, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( situation < 39.35 ) { 
	if( situation < 28.44 ) { 
		if( situation >= 23.3 ) { return new Prediction("2", 4, 0); }
		if( situation < 23.3 ) { return new Prediction("1", 2, 0); }
	}
	if( situation >= 28.44 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 3, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	}
		if( situation >= 39.35 ) { return new Prediction("2", 5, 0); }
	}
	if( "high".equals(stability) ) { 
		if( situation >= 40.25 ) { return new Prediction("3", 10, 0); }
	if( situation < 40.25 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 3, 0); }
		if( situation < 33.92 ) { return new Prediction("3", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
return null;
}
private Prediction runTree46() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 2, 1); }
		if( situation < 33.92 ) { return new Prediction("2", 3, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 4, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 41.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 7, 0); }
	if( "medium".equals(budget) ) { 
	if( situation < 39.35 ) { 
	if( situation < 28.32 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.73 ) { return new Prediction("1", 2, 0); }
		if( situation >= 22.73 ) { return new Prediction("2", 4, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 28.32 ) { 
	if( situation < 34.92 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 31.63 ) { return new Prediction("1", 2, 0); }
		if( situation >= 31.63 ) { return new Prediction("2", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( situation >= 34.92 ) { return new Prediction("1", 6, 0); }
	}
	}
		if( situation >= 39.35 ) { return new Prediction("2", 4, 0); }
	}
	}
	if( situation >= 41.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 43.8 ) { return new Prediction("1", 5, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
	if( "medium".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 44.4 ) { return new Prediction("3", 1, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 2, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 2, 0); }
	if( situation >= 30.59 ) { 
	if( situation >= 33.55 ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 33.55 ) { return new Prediction("3", 2, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
	if( "high".equals(budget) ) { 
		if( situation < 32.61 ) { return new Prediction("3", 2, 0); }
		if( situation >= 32.61 ) { return new Prediction("2", 1, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 3, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
return null;
}
private Prediction runTree47() {
	if( "medium".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 2, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 42 ) { return new Prediction("3", 2, 0); }
		if( situation < 42 ) { return new Prediction("1", 5, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 36.23 ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
		if( situation < 36.23 ) { return new Prediction("1", 3, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 4, 0); }
	}
	if( "low".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 2, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("2", 5, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation < 32.61 ) { return new Prediction("3", 1, 0); }
		if( situation >= 32.61 ) { return new Prediction("2", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 2, 0); }
	if( situation >= 22.39 ) { 
	if( situation < 44.4 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 3, 0); }
	if( situation >= 28.32 ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 2, 0); }
		if( situation < 33.98 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 6, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 3, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("3", 16, 0); }
return null;
}
private Prediction runTree48() {
	if( "medium".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
		if( situation < 25.18 ) { return new Prediction("2", 2, 0); }
		if( situation >= 25.18 ) { return new Prediction("3", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 39 ) { return new Prediction("1", 4, 0); }
	if( situation >= 39 ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 2, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 3, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
	if( situation < 43.8 ) { 
		if( situation >= 36.23 ) { return new Prediction("3", 1, 0); }
		if( situation < 36.23 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	if( "low".equals(approval) ) { 
	if( situation >= 36.8 ) { 
		if( situation < 45 ) { return new Prediction("2", 4, 0); }
	if( situation >= 45 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( situation < 36.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 1, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 1, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 41.4 ) { return new Prediction("2", 3, 0); }
	if( situation >= 41.4 ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 1, 0); }
		if( "high".equals(approval) ) { return new Prediction("2", 2, 1); }
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 31.13 ) { return new Prediction("1", 4, 0); }
		if( situation < 31.13 ) { return new Prediction("2", 2, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 2, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 2, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	if( "high".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 42.35 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 2, 0); }
		if( situation < 33.92 ) { return new Prediction("3", 1, 0); }
	}
		if( situation >= 42.35 ) { return new Prediction("3", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "high".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(approval) ) { return new Prediction("3", 10, 0); }
	}
return null;
}
private Prediction runTree49() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 2, 0); }
	if( "high".equals(stability) ) { 
	if( situation >= 33.92 ) { 
		if( situation < 42.35 ) { return new Prediction("2", 2, 1); }
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 33.92 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "high".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 39 ) { return new Prediction("1", 9, 0); }
	if( situation >= 39 ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 5, 0); }
		if( situation < 42.6 ) { return new Prediction("3", 1, 0); }
	}
	}
	if( "medium".equals(budget) ) { 
	if( situation >= 23.3 ) { 
	if( situation >= 28.65 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 44.4 ) { return new Prediction("2", 3, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 2, 0); }
	if( situation < 36.8 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 28.65 ) { return new Prediction("2", 4, 0); }
	}
		if( situation < 23.3 ) { return new Prediction("1", 3, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 9, 0); }
	if( situation < 33.55 ) { 
		if( situation < 25.19 ) { return new Prediction("2", 1, 0); }
		if( situation >= 25.19 ) { return new Prediction("3", 4, 0); }
	}
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 8, 0); }
	}
return null;
}
private Prediction runTree50() {
	if( "medium".equals(approval) ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 2, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 3, 0); }
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 33.92 ) { 
		if( situation < 42.35 ) { return new Prediction("2", 4, 1); }
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 33.92 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
	}
	if( "high".equals(approval) ) { 
	if( situation < 41.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 8, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 28.44 ) { return new Prediction("2", 3, 0); }
	if( situation >= 28.44 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 33.27 ) { 
		if( situation < 34.92 ) { return new Prediction("2", 3, 1); }
		if( situation >= 34.92 ) { return new Prediction("1", 3, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 4, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
	}
	}
	if( situation >= 41.4 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 44.4 ) { return new Prediction("2", 2, 1); }
	if( situation >= 44.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 2, 1); }
		if( "medium".equals(budget) ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 3, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 3, 0); }
	if( situation < 36.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 1, 0); }
	if( situation >= 30.59 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 3, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 6, 0); }
	}
return null;
}
private Prediction runTree51() {
	if( "medium".equals(approval) ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 2, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 31.98 ) { return new Prediction("3", 6, 0); }
		if( situation < 31.98 ) { return new Prediction("2", 4, 2); }
	}
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 37.8 ) { return new Prediction("1", 3, 0); }
	if( situation >= 37.8 ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "medium".equals(budget) ) { 
		if( situation < 22.73 ) { return new Prediction("1", 2, 0); }
	if( situation >= 22.73 ) { 
	if( situation < 44.4 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 2, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 3, 1); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 38.83 ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 5, 0); }
	if( "low".equals(stability) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 2, 0); }
	if( situation >= 28.1 ) { 
		if( situation < 31.63 ) { return new Prediction("1", 2, 0); }
	if( situation >= 31.63 ) { 
		if( situation < 34.92 ) { return new Prediction("1", 3, 1); }
		if( situation >= 34.92 ) { return new Prediction("1", 2, 0); }
	}
	}
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 38.83 ) { 
	if( "medium".equals(stability) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 2, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 1, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 7, 0); }
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 2, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "medium".equals(stability) ) { 
		if( situation < 25.19 ) { return new Prediction("2", 3, 0); }
	if( situation >= 25.19 ) { 
		if( situation >= 28.28 ) { return new Prediction("2", 3, 0); }
		if( situation < 28.28 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 7, 0); }
	}
return null;
}
private Prediction runTree52() {
	if( "medium".equals(approval) ) { 
	if( situation < 43.5 ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 1, 0); }
	if( situation < 33.92 ) { 
		if( situation < 28.55 ) { return new Prediction("3", 2, 0); }
		if( situation >= 28.55 ) { return new Prediction("3", 3, 1); }
	}
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
		if( situation >= 43.5 ) { return new Prediction("2", 3, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 42 ) { return new Prediction("1", 3, 1); }
		if( situation < 42 ) { return new Prediction("1", 2, 0); }
	}
	if( "medium".equals(budget) ) { 
		if( situation < 44.4 ) { return new Prediction("3", 1, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 9, 0); }
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 31 ) { return new Prediction("1", 1, 0); }
	if( situation >= 31 ) { 
		if( situation < 45 ) { return new Prediction("3", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
	}
	if( "medium".equals(budget) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 2, 0); }
	if( situation >= 28.1 ) { 
	if( situation < 34.92 ) { 
		if( situation >= 33.27 ) { return new Prediction("1", 3, 1); }
		if( situation < 33.27 ) { return new Prediction("1", 2, 0); }
	}
		if( situation >= 34.92 ) { return new Prediction("1", 4, 0); }
	}
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 4, 0); }
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 2, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 2, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 28.86 ) { return new Prediction("2", 11, 0); }
	if( situation < 28.86 ) { 
		if( situation < 25.19 ) { return new Prediction("2", 1, 0); }
		if( situation >= 25.19 ) { return new Prediction("3", 2, 0); }
	}
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 5, 0); }
	}
return null;
}
private Prediction runTree53() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 1, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 40.2 ) { return new Prediction("1", 4, 0); }
	if( situation >= 40.2 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 3, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 3, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(approval) ) { 
		if( situation < 31.17 ) { return new Prediction("2", 6, 0); }
	if( situation >= 31.17 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 2, 0); }
	if( "five".equals(tier) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 4, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 3, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 44.4 ) { return new Prediction("2", 7, 0); }
	if( situation >= 44.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 2, 0); }
	if( situation >= 28.1 ) { 
	if( situation < 34.92 ) { 
		if( situation >= 33.27 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.27 ) { return new Prediction("1", 2, 0); }
	}
		if( situation >= 34.92 ) { return new Prediction("1", 3, 0); }
	}
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 3, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
	if( "high".equals(stability) ) { 
	if( situation >= 37.15 ) { 
		if( situation >= 40.25 ) { return new Prediction("3", 9, 0); }
		if( situation < 40.25 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 37.15 ) { return new Prediction("3", 9, 0); }
	}
return null;
}
private Prediction runTree54() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 1, 0); }
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 39 ) { return new Prediction("1", 2, 0); }
		if( situation >= 39 ) { return new Prediction("2", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 3, 0); }
	if( situation < 46.8 ) { 
	if( situation >= 36.23 ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 36.23 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "five".equals(tier) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 4, 0); }
	if( situation < 33.55 ) { 
		if( situation < 30.59 ) { return new Prediction("2", 3, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("2", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 3, 0); }
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 28.32 ) { return new Prediction("2", 1, 0); }
	if( situation >= 28.32 ) { 
	if( situation >= 33.98 ) { 
		if( situation < 44.4 ) { return new Prediction("2", 2, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 33.98 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 35.15 ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
	if( situation >= 28.1 ) { 
		if( situation < 31.63 ) { return new Prediction("1", 1, 0); }
		if( situation >= 31.63 ) { return new Prediction("2", 3, 1); }
	}
	}
		if( situation >= 35.15 ) { return new Prediction("1", 4, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 42.35 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 2, 1); }
		if( situation < 33.92 ) { return new Prediction("3", 3, 1); }
	}
		if( situation >= 42.35 ) { return new Prediction("3", 3, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 3, 0); }
	}
return null;
}
private Prediction runTree55() {
	if( "high".equals(budget) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 7, 0); }
	if( situation < 36.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 4, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 37.8 ) { return new Prediction("1", 3, 0); }
	if( situation >= 37.8 ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 3, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 35.92 ) { return new Prediction("1", 2, 0); }
		if( situation >= 35.92 ) { return new Prediction("3", 11, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 31.52 ) { return new Prediction("1", 1, 0); }
		if( situation >= 31.52 ) { return new Prediction("3", 2, 0); }
	}
	if( "four".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 2, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 2, 0); }
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "medium".equals(budget) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 3, 0); }
	if( "high".equals(approval) ) { 
	if( situation < 39.35 ) { 
	if( situation < 34.92 ) { 
	if( situation >= 23.3 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 5, 0); }
	if( situation >= 28.32 ) { 
		if( situation >= 33.2 ) { return new Prediction("2", 2, 0); }
		if( situation < 33.2 ) { return new Prediction("1", 3, 0); }
	}
	}
		if( situation < 23.3 ) { return new Prediction("1", 3, 0); }
	}
		if( situation >= 34.92 ) { return new Prediction("1", 7, 0); }
	}
	if( situation >= 39.35 ) { 
	if( situation < 44.4 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 1, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 2, 1); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("3", 6, 0); }
	}
return null;
}
private Prediction runTree56() {
		if( "medium".equals(approval) ) { return new Prediction("3", 15, 0); }
	if( "high".equals(approval) ) { 
	if( "medium".equals(stability) ) { 
		if( situation < 39 ) { return new Prediction("1", 13, 0); }
	if( situation >= 39 ) { 
	if( situation >= 42.6 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 3, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
	if( situation < 42.6 ) { 
		if( situation < 40.8 ) { return new Prediction("2", 1, 0); }
		if( situation >= 40.8 ) { return new Prediction("3", 3, 0); }
	}
	}
	}
	if( "low".equals(stability) ) { 
	if( situation < 39.95 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.39 ) { 
		if( situation >= 27.98 ) { return new Prediction("1", 1, 0); }
		if( situation < 27.98 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
	if( situation >= 28.1 ) { 
	if( situation >= 33.27 ) { 
		if( situation < 35.15 ) { return new Prediction("2", 1, 0); }
		if( situation >= 35.15 ) { return new Prediction("1", 2, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 2, 0); }
	}
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 3, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
		if( situation >= 39.95 ) { return new Prediction("2", 3, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( "low".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
	if( "five".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 3, 0); }
	}
	if( "two".equals(tier) ) { 
		if( situation >= 39.23 ) { return new Prediction("3", 1, 0); }
		if( situation < 39.23 ) { return new Prediction("2", 1, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 6, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
return null;
}
private Prediction runTree57() {
	if( "medium".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 3, 0); }
	if( "low".equals(budget) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 1, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 39 ) { return new Prediction("1", 3, 0); }
	if( situation >= 39 ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 2, 0); }
	}
	if( "five".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 2, 0); }
		if( "low".equals(approval) ) { return new Prediction("2", 3, 0); }
	}
	if( "two".equals(tier) ) { 
		if( situation < 25.23 ) { return new Prediction("1", 1, 0); }
	if( situation >= 25.23 ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 3, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "one".equals(tier) ) { 
		if( situation >= 43.02 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.02 ) { return new Prediction("3", 3, 0); }
	}
	if( "four".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 2, 0); }
		if( "low".equals(approval) ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 2, 0); }
	if( "high".equals(approval) ) { 
	if( situation < 41.4 ) { 
	if( situation >= 27.98 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.98 ) { return new Prediction("1", 3, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 34.92 ) { 
		if( situation < 31.63 ) { return new Prediction("1", 1, 0); }
		if( situation >= 31.63 ) { return new Prediction("1", 3, 1); }
	}
		if( situation >= 34.92 ) { return new Prediction("1", 5, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	if( situation < 27.98 ) { 
		if( situation >= 23.3 ) { return new Prediction("2", 3, 0); }
		if( situation < 23.3 ) { return new Prediction("1", 2, 0); }
	}
	}
	if( situation >= 41.4 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 44.4 ) { return new Prediction("3", 2, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 37.17 ) { return new Prediction("3", 1, 0); }
		if( situation < 37.17 ) { return new Prediction("2", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 7, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
return null;
}
private Prediction runTree58() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 1, 0); }
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 42 ) { return new Prediction("1", 2, 1); }
		if( situation < 42 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
	if( situation < 43.8 ) { 
		if( situation >= 36.23 ) { return new Prediction("3", 3, 0); }
		if( situation < 36.23 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 43.02 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.02 ) { return new Prediction("3", 2, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 4, 0); }
	}
	}
	if( "low".equals(stability) ) { 
		if( situation < 28.32 ) { return new Prediction("2", 6, 0); }
	if( situation >= 28.32 ) { 
	if( situation >= 38.78 ) { 
	if( situation < 44.4 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 41.4 ) { return new Prediction("2", 1, 0); }
		if( situation >= 41.4 ) { return new Prediction("3", 5, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	if( situation >= 44.4 ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 3, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 2, 0); }
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( situation < 38.78 ) { return new Prediction("1", 11, 0); }
	}
	}
		if( "high".equals(stability) ) { return new Prediction("3", 20, 0); }
return null;
}
private Prediction runTree59() {
	if( "medium".equals(approval) ) { 
		if( situation >= 49.2 ) { return new Prediction("2", 1, 0); }
	if( situation < 49.2 ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 2, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 31.98 ) { return new Prediction("3", 9, 0); }
		if( situation < 31.98 ) { return new Prediction("2", 3, 1); }
	}
	}
	}
	if( "high".equals(approval) ) { 
	if( situation < 41.4 ) { 
	if( situation >= 26.93 ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39 ) { return new Prediction("2", 1, 0); }
	}
	if( "medium".equals(budget) ) { 
		if( situation < 28.32 ) { return new Prediction("2", 2, 0); }
	if( situation >= 28.32 ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.98 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 34.92 ) { return new Prediction("2", 5, 0); }
		if( situation >= 34.92 ) { return new Prediction("1", 3, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( situation < 26.93 ) { return new Prediction("1", 7, 0); }
	}
	if( situation >= 41.4 ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 4, 0); }
	if( "low".equals(stability) ) { 
		if( situation < 44.4 ) { return new Prediction("3", 3, 1); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 2, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 6, 0); }
	if( situation < 33.55 ) { 
	if( situation < 30.59 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 4, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( situation >= 30.59 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 7, 0); }
	}
return null;
}
private Prediction runTree60() {
	if( "medium".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 40.8 ) { 
		if( situation < 39 ) { return new Prediction("1", 4, 0); }
		if( situation >= 39 ) { return new Prediction("2", 2, 0); }
	}
	if( situation >= 40.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 3, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(approval) ) { 
		if( situation >= 46.2 ) { return new Prediction("1", 2, 0); }
	if( situation < 46.2 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "five".equals(tier) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 3, 0); }
	if( situation < 33.55 ) { 
		if( situation < 30.59 ) { return new Prediction("2", 1, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	}
	}
	if( "low".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 2, 0); }
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 41.4 ) { return new Prediction("2", 6, 0); }
		if( situation >= 41.4 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 11, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 3, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 42.35 ) { return new Prediction("2", 4, 2); }
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 4, 0); }
	}
		if( "high".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(approval) ) { return new Prediction("3", 2, 0); }
	}
return null;
}
private Prediction runTree61() {
	if( "high".equals(budget) ) { 
	if( situation >= 33.55 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 2, 0); }
		if( situation >= 45 ) { return new Prediction("1", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("2", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
	if( "low".equals(budget) ) { 
	if( "medium".equals(stability) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 3, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
	if( situation < 43.8 ) { 
		if( situation >= 36.23 ) { return new Prediction("3", 2, 0); }
		if( situation < 36.23 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 33.92 ) { 
		if( situation < 42.35 ) { return new Prediction("3", 3, 1); }
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 33.92 ) { return new Prediction("3", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 3, 0); }
	}
	}
	if( "medium".equals(budget) ) { 
		if( "medium".equals(stability) ) { return new Prediction("2", 1, 0); }
	if( "low".equals(stability) ) { 
		if( situation < 28.32 ) { return new Prediction("2", 3, 0); }
	if( situation >= 28.32 ) { 
	if( situation < 39.35 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 2, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	if( situation >= 39.35 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 41.4 ) { return new Prediction("2", 1, 0); }
		if( situation >= 41.4 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 3, 0); }
	}
	}
	}
		if( "high".equals(stability) ) { return new Prediction("3", 6, 0); }
	}
return null;
}
private Prediction runTree62() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 3, 0); }
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 2, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.92 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 5, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 2, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 41.4 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 2, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 3, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 6, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 29.75 ) { return new Prediction("2", 2, 0); }
	if( situation >= 29.75 ) { 
	if( situation < 35.15 ) { 
		if( situation >= 33.27 ) { return new Prediction("1", 2, 1); }
		if( situation < 33.27 ) { return new Prediction("1", 2, 0); }
	}
		if( situation >= 35.15 ) { return new Prediction("1", 3, 0); }
	}
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "four".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 1, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( situation >= 41.4 ) { 
		if( situation >= 43.8 ) { return new Prediction("1", 5, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 4, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( situation < 45 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 2, 0); }
	if( "five".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 5, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 2, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
	if( "one".equals(tier) ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 3, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
return null;
}
private Prediction runTree63() {
	if( "medium".equals(stability) ) { 
	if( situation < 38.17 ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 1, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 1, 0); }
	if( "low".equals(approval) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 6, 0); }
	if( situation < 36.8 ) { 
	if( situation < 35.42 ) { 
		if( situation < 31.17 ) { return new Prediction("2", 3, 0); }
	if( situation >= 31.17 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 2, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( situation >= 35.42 ) { return new Prediction("3", 1, 0); }
	}
	}
	}
	if( situation >= 38.17 ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(approval) ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 4, 0); }
	if( situation < 42.6 ) { 
		if( situation < 39 ) { return new Prediction("1", 2, 0); }
		if( situation >= 39 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("2", 4, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 42.6 ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 2, 0); }
	}
		if( situation < 42.6 ) { return new Prediction("2", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 7, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 29.75 ) { return new Prediction("2", 1, 0); }
	if( situation >= 29.75 ) { 
	if( situation < 34.92 ) { 
		if( situation >= 33.27 ) { return new Prediction("1", 2, 1); }
		if( situation < 33.27 ) { return new Prediction("1", 2, 0); }
	}
		if( situation >= 34.92 ) { return new Prediction("1", 4, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(stability) ) { 
		if( situation < 38.53 ) { return new Prediction("3", 12, 0); }
	if( situation >= 38.53 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 42.35 ) { return new Prediction("2", 3, 0); }
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
return null;
}
private Prediction runTree64() {
	if( "high".equals(budget) ) { 
	if( situation < 39.42 ) { 
		if( situation < 30.59 ) { return new Prediction("2", 2, 0); }
	if( situation >= 30.59 ) { 
	if( situation >= 33.55 ) { 
		if( situation < 35.42 ) { return new Prediction("2", 1, 0); }
		if( situation >= 35.42 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
	}
	if( situation >= 39.42 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 46.2 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.2 ) { return new Prediction("2", 3, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "low".equals(budget) ) { 
	if( "medium".equals(approval) ) { 
		if( situation >= 33.27 ) { return new Prediction("3", 9, 0); }
	if( situation < 33.27 ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 5, 2); }
	}
	}
	if( "high".equals(approval) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 4, 0); }
	if( situation < 46.8 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 40.8 ) { return new Prediction("1", 1, 0); }
		if( situation >= 40.8 ) { return new Prediction("3", 4, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 31 ) { return new Prediction("1", 1, 0); }
	if( situation >= 31 ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	if( "medium".equals(budget) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 1, 0); }
	if( "high".equals(approval) ) { 
	if( situation < 35.15 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 3, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 33.27 ) { return new Prediction("2", 3, 1); }
		if( situation < 33.27 ) { return new Prediction("1", 3, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 35.15 ) { 
	if( situation >= 42 ) { 
		if( situation < 44.4 ) { return new Prediction("3", 3, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 3, 0); }
	}
		if( situation < 42 ) { return new Prediction("1", 9, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("3", 7, 0); }
	}
return null;
}
private Prediction runTree65() {
	if( "medium".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 1, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 39 ) { return new Prediction("1", 6, 0); }
	if( situation >= 39 ) { 
		if( situation < 40.8 ) { return new Prediction("2", 2, 0); }
	if( situation >= 40.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 3, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( situation >= 33.55 ) { 
	if( situation >= 46.2 ) { 
		if( situation >= 48.6 ) { return new Prediction("2", 2, 0); }
		if( situation < 48.6 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 46.2 ) { return new Prediction("2", 5, 0); }
	}
		if( situation < 33.55 ) { return new Prediction("3", 5, 0); }
	}
	}
	if( "low".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 1, 0); }
	if( "high".equals(approval) ) { 
	if( situation < 39.35 ) { 
		if( situation >= 34.58 ) { return new Prediction("1", 10, 0); }
	if( situation < 34.58 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.73 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.73 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.32 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 4, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( situation >= 39.35 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 41.4 ) { return new Prediction("2", 2, 0); }
		if( situation >= 41.4 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 37.17 ) { return new Prediction("3", 1, 0); }
		if( situation < 37.17 ) { return new Prediction("2", 2, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 9, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
return null;
}
private Prediction runTree66() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 33.92 ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 3, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 42.35 ) { return new Prediction("2", 3, 0); }
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	if( situation < 33.92 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("2", 4, 2); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 44.4 ) { 
	if( "three".equals(tier) ) { 
	if( situation < 39 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 2, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 2, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( situation >= 39 ) { return new Prediction("2", 3, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 28.1 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 3, 0); }
	}
		if( situation >= 28.1 ) { return new Prediction("1", 5, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 2, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 2, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("2", 3, 0); }
	}
	if( situation >= 44.4 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
	if( situation < 46.8 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
	if( "five".equals(tier) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 3, 0); }
	if( situation < 33.55 ) { 
		if( situation >= 27.5 ) { return new Prediction("3", 1, 0); }
		if( situation < 27.5 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 43.02 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.02 ) { return new Prediction("3", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 6, 0); }
	}
return null;
}
private Prediction runTree67() {
	if( "high".equals(budget) ) { 
		if( situation >= 46.2 ) { return new Prediction("1", 2, 0); }
	if( situation < 46.2 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 3, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 5, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(budget) ) { 
	if( "medium".equals(stability) ) { 
		if( situation < 39 ) { return new Prediction("1", 10, 0); }
	if( situation >= 39 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 2, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("3", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 33.27 ) { return new Prediction("3", 13, 0); }
		if( situation < 33.27 ) { return new Prediction("3", 5, 1); }
	}
	}
	if( "medium".equals(budget) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(approval) ) { 
	if( situation >= 34.58 ) { 
	if( situation >= 38.78 ) { 
	if( "three".equals(tier) ) { 
	if( situation < 44.4 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 1, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 2, 1); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( situation < 38.78 ) { 
		if( situation < 34.92 ) { return new Prediction("1", 3, 1); }
		if( situation >= 34.92 ) { return new Prediction("1", 6, 0); }
	}
	}
	if( situation < 34.58 ) { 
	if( situation < 32.1 ) { 
	if( situation < 28.44 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( situation >= 28.44 ) { return new Prediction("1", 3, 0); }
	}
		if( situation >= 32.1 ) { return new Prediction("2", 3, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("3", 3, 0); }
	}
return null;
}
private Prediction runTree68() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 42.35 ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 4, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 33.92 ) { return new Prediction("3", 3, 0); }
		if( situation < 33.92 ) { return new Prediction("2", 6, 3); }
	}
	}
		if( situation >= 42.35 ) { return new Prediction("2", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 6, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 39.6 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 3, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( situation >= 39.6 ) { 
		if( situation < 44.4 ) { return new Prediction("3", 1, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 4, 1); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 7, 0); }
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 5, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	if( "low".equals(approval) ) { 
	if( "medium".equals(stability) ) { 
	if( situation < 45 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "five".equals(tier) ) { 
	if( situation >= 27.5 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 27.5 ) { return new Prediction("2", 2, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	if( situation >= 45 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 10, 0); }
	}
return null;
}
private Prediction runTree69() {
	if( "high".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 2, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 1, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("2", 7, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 4, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 4, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	if( "low".equals(budget) ) { 
	if( "medium".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 33.38 ) { 
		if( situation < 39 ) { return new Prediction("1", 2, 0); }
	if( situation >= 39 ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 2, 1); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( situation < 33.38 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 40.25 ) { return new Prediction("3", 9, 0); }
	if( situation < 40.25 ) { 
	if( situation >= 31.98 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 31.98 ) { return new Prediction("2", 3, 1); }
	}
	}
	}
	if( "medium".equals(budget) ) { 
		if( "medium".equals(stability) ) { return new Prediction("2", 2, 0); }
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 41.4 ) { return new Prediction("2", 2, 0); }
		if( situation >= 41.4 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 34.92 ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
	if( situation >= 28.1 ) { 
		if( situation >= 33.27 ) { return new Prediction("2", 3, 1); }
		if( situation < 33.27 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( situation >= 34.92 ) { return new Prediction("1", 4, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 2, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "high".equals(stability) ) { return new Prediction("3", 5, 0); }
	}
return null;
}
private Prediction runTree70() {
	if( "medium".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
		if( situation < 25.18 ) { return new Prediction("2", 1, 0); }
		if( situation >= 25.18 ) { return new Prediction("3", 1, 0); }
	}
	if( "high".equals(approval) ) { 
		if( situation < 39 ) { return new Prediction("1", 5, 0); }
	if( situation >= 39 ) { 
	if( situation >= 43.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( situation < 43.8 ) { 
		if( situation < 40.8 ) { return new Prediction("2", 2, 0); }
		if( situation >= 40.8 ) { return new Prediction("3", 1, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 5, 0); }
	if( situation < 33.55 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 3, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 1, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 3, 0); }
	if( situation < 33.98 ) { 
		if( situation >= 27.98 ) { return new Prediction("1", 1, 0); }
		if( situation < 27.98 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 8, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 5, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 4, 0); }
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 4, 0); }
	}
	}
		if( "high".equals(stability) ) { return new Prediction("3", 24, 0); }
return null;
}
private Prediction runTree71() {
	if( "medium".equals(approval) ) { 
	if( "medium".equals(stability) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 2, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("2", 1, 0); }
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 33.92 ) { 
		if( situation < 42.35 ) { return new Prediction("2", 2, 1); }
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 33.92 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 4, 0); }
	}
	}
	if( "high".equals(approval) ) { 
	if( situation >= 26.93 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 4, 0); }
	if( situation >= 28.32 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 38.02 ) { return new Prediction("1", 1, 0); }
	if( situation >= 38.02 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	if( "medium".equals(budget) ) { 
	if( situation < 39.35 ) { 
	if( situation >= 33.27 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 34.92 ) { return new Prediction("2", 1, 0); }
		if( situation >= 34.92 ) { return new Prediction("1", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 8, 0); }
	}
	if( situation >= 39.35 ) { 
	if( "three".equals(tier) ) { 
	if( situation < 44.4 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 2, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 3, 1); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	}
		if( situation < 26.93 ) { return new Prediction("1", 9, 0); }
	}
	if( "low".equals(approval) ) { 
	if( "medium".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 4, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 8, 0); }
	}
return null;
}
private Prediction runTree72() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 28.55 ) { return new Prediction("3", 1, 0); }
	if( situation >= 28.55 ) { 
		if( situation >= 33.92 ) { return new Prediction("3", 4, 1); }
		if( situation < 33.92 ) { return new Prediction("2", 2, 1); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 39.35 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 2, 0); }
	if( situation >= 22.39 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 3, 0); }
		if( situation >= 28.32 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 4, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 12, 0); }
	if( "one".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 2, 0); }
	if( "medium".equals(budget) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 2, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	if( situation >= 39.35 ) { 
	if( situation < 44.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("2", 1, 0); }
	if( "medium".equals(budget) ) { 
		if( situation >= 42 ) { return new Prediction("2", 3, 1); }
		if( situation < 42 ) { return new Prediction("2", 2, 0); }
	}
	}
	if( situation >= 44.4 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 3, 0); }
	if( "five".equals(tier) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 3, 0); }
	if( situation < 33.55 ) { 
		if( situation < 30.59 ) { return new Prediction("2", 2, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 2, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 5, 0); }
	}
return null;
}
private Prediction runTree73() {
	if( "medium".equals(approval) ) { 
		if( situation < 25.18 ) { return new Prediction("2", 1, 0); }
	if( situation >= 25.18 ) { 
		if( situation >= 40.25 ) { return new Prediction("3", 9, 0); }
	if( situation < 40.25 ) { 
	if( situation < 38.45 ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 31.98 ) { return new Prediction("3", 5, 0); }
		if( situation < 31.98 ) { return new Prediction("3", 3, 1); }
	}
	}
	if( situation >= 38.45 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 37.8 ) { return new Prediction("1", 1, 0); }
	if( situation >= 37.8 ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "medium".equals(budget) ) { 
		if( situation >= 30.19 ) { return new Prediction("2", 2, 0); }
		if( situation < 30.19 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 10, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 10, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	if( "low".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 2, 0); }
	}
	if( "five".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 3, 0); }
	if( situation < 33.55 ) { 
		if( situation < 30.59 ) { return new Prediction("2", 2, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 1, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("3", 4, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 4, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 5, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
return null;
}
private Prediction runTree74() {
	if( "medium".equals(approval) ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 4, 0); }
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 42.35 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.92 ) { return new Prediction("2", 5, 1); }
	}
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "high".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 42 ) { return new Prediction("1", 2, 1); }
		if( situation < 42 ) { return new Prediction("1", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 2, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	if( "medium".equals(budget) ) { 
	if( situation < 39.35 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 35.15 ) { 
		if( situation >= 33.27 ) { return new Prediction("1", 4, 2); }
		if( situation < 33.27 ) { return new Prediction("1", 2, 0); }
	}
		if( situation >= 35.15 ) { return new Prediction("1", 3, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	if( situation >= 39.35 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 41.4 ) { return new Prediction("2", 2, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 2, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 7, 0); }
	if( situation < 36.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 3, 0); }
	if( situation >= 30.59 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 3, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 7, 0); }
	}
return null;
}
private Prediction runTree75() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 25.18 ) { return new Prediction("2", 2, 0); }
	if( situation >= 25.18 ) { 
		if( situation < 28.55 ) { return new Prediction("3", 3, 0); }
	if( situation >= 28.55 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 2, 0); }
		if( situation < 33.92 ) { return new Prediction("2", 4, 2); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "medium".equals(stability) ) { 
	if( situation >= 42.6 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	if( situation < 42.6 ) { 
		if( situation < 40.2 ) { return new Prediction("1", 3, 0); }
		if( situation >= 40.2 ) { return new Prediction("3", 2, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( situation < 39.35 ) { 
	if( "three".equals(tier) ) { 
	if( situation < 28.32 ) { 
		if( situation < 22.39 ) { return new Prediction("1", 3, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 2, 0); }
	}
		if( situation >= 28.32 ) { return new Prediction("1", 5, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 34.92 ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
	if( situation >= 28.1 ) { 
		if( situation < 31.63 ) { return new Prediction("1", 1, 0); }
		if( situation >= 31.63 ) { return new Prediction("2", 3, 1); }
	}
	}
		if( situation >= 34.92 ) { return new Prediction("1", 2, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 2, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	if( situation >= 39.35 ) { 
		if( situation < 44.4 ) { return new Prediction("2", 3, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( "low".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 2, 0); }
		if( situation >= 45 ) { return new Prediction("1", 3, 0); }
	}
	if( "five".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("2", 4, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 1, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 3, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
return null;
}
private Prediction runTree76() {
	if( "medium".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 2, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 2, 0); }
	}
	if( "high".equals(approval) ) { 
		if( situation < 39 ) { return new Prediction("1", 4, 0); }
	if( situation >= 39 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
	if( situation < 46.8 ) { 
	if( situation >= 42.6 ) { 
		if( "three".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 5, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( situation < 42.6 ) { return new Prediction("2", 3, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 5, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 2, 0); }
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 44.4 ) { 
		if( situation < 35.74 ) { return new Prediction("2", 1, 0); }
		if( situation >= 35.74 ) { return new Prediction("2", 2, 1); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 4, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 9, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 5, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(stability) ) { 
		if( situation < 29.07 ) { return new Prediction("2", 2, 1); }
		if( situation >= 29.07 ) { return new Prediction("3", 23, 0); }
	}
return null;
}
private Prediction runTree77() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 1, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 2, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
	if( "low".equals(stability) ) { 
		if( situation < 41.4 ) { return new Prediction("2", 8, 0); }
	if( situation >= 41.4 ) { 
		if( situation < 44.4 ) { return new Prediction("3", 1, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 28.1 ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 2, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 28.1 ) { 
	if( situation >= 33.27 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
	if( situation < 46.8 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("2", 1, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 35.15 ) { return new Prediction("1", 2, 1); }
		if( situation >= 35.15 ) { return new Prediction("1", 4, 0); }
	}
	}
	}
		if( situation < 33.27 ) { return new Prediction("1", 5, 0); }
	}
	}
	if( "one".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 3, 0); }
	if( "medium".equals(budget) ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 3, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 2, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 2, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
	if( "five".equals(tier) ) { 
		if( situation >= 27.5 ) { return new Prediction("3", 1, 0); }
		if( situation < 27.5 ) { return new Prediction("2", 2, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 4, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 5, 0); }
	}
return null;
}
private Prediction runTree78() {
	if( "medium".equals(stability) ) { 
	if( situation >= 42.6 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 44.4 ) { return new Prediction("2", 1, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 5, 0); }
	}
	if( "five".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 2, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	if( "two".equals(tier) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	if( situation < 42.6 ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 1, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 39 ) { return new Prediction("1", 2, 0); }
		if( situation >= 39 ) { return new Prediction("2", 1, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("2", 2, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 25.23 ) { return new Prediction("1", 2, 0); }
	if( situation >= 25.23 ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( situation < 39.35 ) { 
	if( situation < 28.32 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 28.32 ) { 
		if( situation >= 34.58 ) { return new Prediction("1", 11, 0); }
	if( situation < 34.58 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
		if( situation >= 39.35 ) { return new Prediction("2", 5, 0); }
	}
	if( "high".equals(stability) ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 8, 4); }
		if( "five".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 3, 0); }
	}
return null;
}
private Prediction runTree79() {
	if( "medium".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(approval) ) { 
		if( situation < 40.2 ) { return new Prediction("1", 9, 0); }
	if( situation >= 40.2 ) { 
		if( "three".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 2, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(approval) ) { 
		if( situation >= 46.2 ) { return new Prediction("1", 3, 0); }
	if( situation < 46.2 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "five".equals(tier) ) { 
	if( situation >= 27.5 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 3, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 2, 0); }
	}
		if( situation < 27.5 ) { return new Prediction("2", 3, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
	}
	}
	if( "low".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 3, 0); }
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 44.4 ) { 
		if( situation < 22.73 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.73 ) { return new Prediction("2", 6, 0); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 3, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 29.75 ) { return new Prediction("2", 1, 0); }
		if( situation >= 29.75 ) { return new Prediction("1", 5, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(stability) ) { 
		if( situation < 38.53 ) { return new Prediction("3", 9, 0); }
	if( situation >= 38.53 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 4, 2); }
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
return null;
}
private Prediction runTree80() {
	if( "medium".equals(approval) ) { 
		if( situation >= 49.2 ) { return new Prediction("2", 3, 0); }
	if( situation < 49.2 ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 3, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 1, 0); }
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 42.35 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 3, 1); }
		if( situation < 33.92 ) { return new Prediction("3", 3, 1); }
	}
		if( situation >= 42.35 ) { return new Prediction("3", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
	}
	}
	if( "high".equals(approval) ) { 
	if( situation < 39.95 ) { 
	if( situation < 28.32 ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 2, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 5, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 28.32 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 6, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 33.27 ) { 
		if( situation < 34.92 ) { return new Prediction("1", 2, 1); }
		if( situation >= 34.92 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 3, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	}
	if( situation >= 39.95 ) { 
	if( situation < 44.4 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 2, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	if( situation >= 44.4 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
	if( situation < 46.8 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("2", 3, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "medium".equals(stability) ) { 
		if( situation >= 46.2 ) { return new Prediction("1", 1, 0); }
	if( situation < 46.2 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 5, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 8, 0); }
	}
return null;
}
private Prediction runTree81() {
	if( "medium".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
		if( situation < 25.18 ) { return new Prediction("2", 1, 0); }
		if( situation >= 25.18 ) { return new Prediction("3", 3, 0); }
	}
	if( "high".equals(approval) ) { 
		if( situation < 35.02 ) { return new Prediction("1", 5, 0); }
	if( situation >= 35.02 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 2, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 3, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("2", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 43.02 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.02 ) { return new Prediction("3", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 5, 0); }
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 35.78 ) { 
		if( situation < 44.4 ) { return new Prediction("3", 3, 1); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	if( situation < 35.78 ) { 
		if( situation < 22.73 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.73 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.32 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 2, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 5, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 4, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 3, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(stability) ) { 
		if( situation >= 31.98 ) { return new Prediction("3", 14, 0); }
		if( situation < 31.98 ) { return new Prediction("2", 2, 1); }
	}
return null;
}
private Prediction runTree82() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 6, 0); }
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 33.92 ) { 
		if( situation < 42.35 ) { return new Prediction("2", 2, 0); }
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 33.92 ) { return new Prediction("3", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 3, 0); }
	}
	if( "high".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 39 ) { return new Prediction("1", 5, 0); }
	if( situation >= 39 ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 3, 1); }
		if( situation < 42.6 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	if( "medium".equals(budget) ) { 
	if( situation < 28.32 ) { 
		if( situation >= 23.3 ) { return new Prediction("2", 5, 0); }
		if( situation < 23.3 ) { return new Prediction("1", 1, 0); }
	}
	if( situation >= 28.32 ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 33.98 ) { 
	if( situation < 44.4 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 2, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 4, 1); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 33.98 ) { return new Prediction("1", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 34.92 ) { 
		if( situation < 31.63 ) { return new Prediction("1", 1, 0); }
		if( situation >= 31.63 ) { return new Prediction("1", 3, 1); }
	}
		if( situation >= 34.92 ) { return new Prediction("1", 3, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
	if( "five".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 2, 0); }
	if( situation < 33.55 ) { 
		if( situation >= 27.5 ) { return new Prediction("3", 1, 0); }
		if( situation < 27.5 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 1, 0); }
	}
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 3, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
return null;
}
private Prediction runTree83() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 42.35 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 5, 2); }
		if( situation < 33.92 ) { return new Prediction("3", 5, 2); }
	}
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 41.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 39 ) { return new Prediction("1", 3, 0); }
		if( situation >= 39 ) { return new Prediction("2", 1, 0); }
	}
	if( "medium".equals(budget) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 28.32 ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 2, 0); }
	}
		if( situation >= 28.32 ) { return new Prediction("1", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 5, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 4, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	}
	if( situation >= 41.4 ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 4, 2); }
		if( "low".equals(stability) ) { return new Prediction("2", 1, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 1, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 2, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 2, 0); }
	if( situation >= 30.59 ) { 
		if( situation < 38.75 ) { return new Prediction("3", 3, 0); }
		if( situation >= 38.75 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 4, 0); }
	}
return null;
}
private Prediction runTree84() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 38.45 ) { return new Prediction("3", 11, 0); }
	if( situation >= 38.45 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 42.35 ) { return new Prediction("2", 2, 1); }
		if( situation >= 42.35 ) { return new Prediction("3", 2, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 41.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 39 ) { return new Prediction("1", 4, 0); }
		if( situation >= 39 ) { return new Prediction("2", 3, 0); }
	}
	if( "medium".equals(budget) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( situation >= 41.4 ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 2, 1); }
	if( "low".equals(stability) ) { 
		if( situation < 44.4 ) { return new Prediction("3", 4, 1); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 32.8 ) { return new Prediction("1", 1, 0); }
	if( situation >= 32.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 7, 0); }
	}
	if( "one".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 1, 0); }
	if( "low".equals(stability) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 3, 0); }
	if( situation < 36.23 ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
		if( situation >= 32.1 ) { return new Prediction("2", 3, 0); }
	}
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "five".equals(tier) ) { 
		if( situation >= 27.5 ) { return new Prediction("3", 5, 0); }
		if( situation < 27.5 ) { return new Prediction("2", 1, 0); }
	}
	if( "two".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 3, 0); }
	}
	if( "one".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 2, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
return null;
}
private Prediction runTree85() {
	if( "medium".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 44.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 39 ) { return new Prediction("1", 4, 0); }
		if( situation >= 39 ) { return new Prediction("2", 2, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
		if( situation >= 44.4 ) { return new Prediction("3", 4, 1); }
	}
	if( "five".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 3, 0); }
		if( "low".equals(approval) ) { return new Prediction("2", 3, 0); }
	}
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
	if( situation < 43.8 ) { 
		if( situation < 31 ) { return new Prediction("1", 1, 0); }
		if( situation >= 31 ) { return new Prediction("3", 2, 0); }
	}
	}
	if( "one".equals(tier) ) { 
		if( situation < 30.45 ) { return new Prediction("1", 2, 0); }
	if( situation >= 30.45 ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 3, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	if( "low".equals(stability) ) { 
	if( situation >= 23.3 ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 3, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 28.99 ) { return new Prediction("2", 2, 0); }
	if( situation >= 28.99 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 33.27 ) { 
		if( situation >= 40.13 ) { return new Prediction("1", 1, 0); }
		if( situation < 40.13 ) { return new Prediction("2", 2, 0); }
	}
		if( situation < 33.27 ) { return new Prediction("1", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 3, 0); }
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 2, 0); }
	}
	}
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
		if( situation < 23.3 ) { return new Prediction("1", 4, 0); }
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 42.35 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 2, 1); }
		if( situation < 33.92 ) { return new Prediction("2", 5, 2); }
	}
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 5, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 4, 0); }
	}
return null;
}
private Prediction runTree86() {
	if( "medium".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 32.17 ) { 
	if( situation >= 42 ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("3", 1, 0); }
		if( "low".equals(approval) ) { return new Prediction("1", 2, 0); }
	}
		if( situation < 42 ) { return new Prediction("1", 2, 0); }
	}
		if( situation < 32.17 ) { return new Prediction("3", 1, 0); }
	}
	if( "five".equals(tier) ) { 
		if( situation < 38.75 ) { return new Prediction("3", 1, 0); }
		if( situation >= 38.75 ) { return new Prediction("2", 1, 0); }
	}
	if( "two".equals(tier) ) { 
	if( situation < 45 ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("3", 4, 0); }
		if( "low".equals(approval) ) { return new Prediction("2", 1, 0); }
	}
		if( situation >= 45 ) { return new Prediction("1", 2, 0); }
	}
	if( "one".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 1, 0); }
	if( "low".equals(approval) ) { 
		if( situation >= 38.84 ) { return new Prediction("2", 1, 0); }
		if( situation < 38.84 ) { return new Prediction("3", 1, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 4, 0); }
		if( "low".equals(approval) ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( situation < 39.95 ) { 
	if( situation < 34.92 ) { 
	if( situation >= 23.3 ) { 
	if( situation >= 27.98 ) { 
	if( situation >= 33.2 ) { 
		if( situation >= 34.58 ) { return new Prediction("1", 3, 1); }
		if( situation < 34.58 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 33.2 ) { return new Prediction("1", 3, 0); }
	}
		if( situation < 27.98 ) { return new Prediction("2", 4, 0); }
	}
		if( situation < 23.3 ) { return new Prediction("1", 3, 0); }
	}
		if( situation >= 34.92 ) { return new Prediction("1", 9, 0); }
	}
	if( situation >= 39.95 ) { 
	if( situation < 44.4 ) { 
		if( situation >= 42 ) { return new Prediction("2", 4, 2); }
		if( situation < 42 ) { return new Prediction("2", 1, 0); }
	}
	if( situation >= 44.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 3, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 1, 0); }
	}
	}
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.92 ) { return new Prediction("3", 2, 0); }
		if( situation < 33.92 ) { return new Prediction("3", 5, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 6, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
return null;
}
private Prediction runTree87() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 40.25 ) { return new Prediction("3", 4, 0); }
	if( situation < 40.25 ) { 
		if( situation < 28.55 ) { return new Prediction("3", 2, 0); }
	if( situation >= 28.55 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.92 ) { return new Prediction("3", 4, 1); }
		if( situation < 33.92 ) { return new Prediction("2", 4, 2); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
	}
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 44.4 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("2", 1, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
	if( situation >= 22.39 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 4, 0); }
	if( situation >= 28.32 ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 3, 0); }
		if( situation < 33.98 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
	}
		if( situation >= 44.4 ) { return new Prediction("1", 2, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "two".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
	if( situation >= 36.23 ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 36.23 ) { return new Prediction("1", 2, 0); }
	}
	if( "medium".equals(budget) ) { 
		if( situation >= 31.13 ) { return new Prediction("1", 5, 0); }
		if( situation < 31.13 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "four".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 2, 0); }
	if( "low".equals(stability) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 3, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 2, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "medium".equals(stability) ) { 
	if( situation < 34.92 ) { 
	if( situation < 30.59 ) { 
		if( situation >= 28.28 ) { return new Prediction("2", 2, 0); }
	if( situation < 28.28 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( situation >= 30.59 ) { return new Prediction("3", 2, 0); }
	}
		if( situation >= 34.92 ) { return new Prediction("2", 4, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 7, 0); }
	}
return null;
}
private Prediction runTree88() {
	if( "medium".equals(stability) ) { 
	if( "high".equals(budget) ) { 
	if( situation < 34.92 ) { 
	if( situation >= 28.28 ) { 
		if( situation < 30.59 ) { return new Prediction("2", 1, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 28.28 ) { return new Prediction("3", 1, 0); }
	}
	if( situation >= 34.92 ) { 
		if( situation >= 46.2 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.2 ) { return new Prediction("2", 7, 0); }
	}
	}
	if( "low".equals(budget) ) { 
	if( situation < 39 ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 1, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 4, 0); }
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 39 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 43.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 4, 0); }
	}
	if( "low".equals(stability) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 3, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "medium".equals(budget) ) { 
		if( situation >= 43.2 ) { return new Prediction("1", 3, 0); }
	if( situation < 43.2 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 2, 0); }
	if( situation < 33.98 ) { 
		if( situation >= 27.98 ) { return new Prediction("1", 2, 0); }
	if( situation < 27.98 ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 2, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 3, 0); }
	}
	}
	}
	}
	if( "high".equals(stability) ) { 
		if( situation < 33.35 ) { return new Prediction("2", 4, 1); }
		if( situation >= 33.35 ) { return new Prediction("3", 22, 0); }
	}
return null;
}
private Prediction runTree89() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 2, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
	if( situation < 42.35 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.92 ) { return new Prediction("3", 4, 1); }
	}
		if( situation >= 42.35 ) { return new Prediction("3", 2, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 41.4 ) { 
	if( "medium".equals(stability) ) { 
		if( situation < 37.8 ) { return new Prediction("1", 1, 0); }
		if( situation >= 37.8 ) { return new Prediction("2", 1, 0); }
	}
		if( "low".equals(stability) ) { return new Prediction("2", 3, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 41.4 ) { 
		if( situation < 44.4 ) { return new Prediction("3", 3, 1); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 9, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 33.27 ) { 
		if( situation < 34.92 ) { return new Prediction("2", 3, 1); }
	if( situation >= 34.92 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 2, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 5, 0); }
	}
	}
		if( situation < 33.27 ) { return new Prediction("1", 3, 0); }
	}
	if( "one".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 1, 0); }
	if( "medium".equals(budget) ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 1, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 3, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 2, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "five".equals(tier) ) { 
	if( situation < 38.75 ) { 
		if( situation < 30.59 ) { return new Prediction("2", 2, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 1, 0); }
	}
		if( situation >= 38.75 ) { return new Prediction("2", 3, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "one".equals(tier) ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 3, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 6, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
return null;
}
private Prediction runTree90() {
	if( "medium".equals(stability) ) { 
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("2", 2, 0); }
	}
	if( "high".equals(approval) ) { 
		if( situation < 39 ) { return new Prediction("1", 9, 0); }
	if( situation >= 39 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 2, 0); }
	if( situation >= 41.4 ) { 
		if( situation < 44.4 ) { return new Prediction("1", 2, 0); }
	if( situation >= 44.4 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 4, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
	}
	if( "low".equals(approval) ) { 
		if( situation < 34.92 ) { return new Prediction("3", 2, 0); }
		if( situation >= 34.92 ) { return new Prediction("2", 5, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( situation < 39.35 ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 29.75 ) { return new Prediction("2", 2, 0); }
		if( situation >= 29.75 ) { return new Prediction("1", 5, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 3, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( situation >= 39.35 ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 3, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 41.4 ) { return new Prediction("2", 1, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 3, 1); }
	}
	}
	}
	if( "high".equals(stability) ) { 
	if( situation < 29.07 ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 4, 2); }
		if( "high".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(approval) ) { return new Prediction("3", 1, 0); }
	}
	if( situation >= 29.07 ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("3", 7, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 7, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 3, 0); }
	}
	}
return null;
}
private Prediction runTree91() {
	if( "medium".equals(stability) ) { 
	if( "high".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 3, 0); }
	}
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 2, 0); }
	if( situation >= 30.59 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 3, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 1, 0); }
	if( "high".equals(approval) ) { 
		if( situation < 37.8 ) { return new Prediction("1", 2, 0); }
	if( situation >= 37.8 ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 2, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 2, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 2, 0); }
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 4, 0); }
	if( situation < 33.98 ) { 
		if( situation >= 27.98 ) { return new Prediction("1", 1, 0); }
		if( situation < 27.98 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 34.92 ) { return new Prediction("2", 2, 0); }
		if( situation >= 34.92 ) { return new Prediction("1", 7, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 4, 0); }
	if( "four".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 2, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
	}
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
	if( situation < 42.35 ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 2, 1); }
		if( situation < 33.92 ) { return new Prediction("3", 5, 2); }
	}
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
return null;
}
private Prediction runTree92() {
	if( "medium".equals(stability) ) { 
	if( "high".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 45 ) { return new Prediction("2", 1, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 1, 0); }
	if( situation >= 30.59 ) { 
		if( situation < 38.75 ) { return new Prediction("3", 1, 0); }
		if( situation >= 38.75 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 43.02 ) { return new Prediction("2", 1, 0); }
		if( situation < 43.02 ) { return new Prediction("3", 2, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 33.38 ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 3, 0); }
	if( situation < 42.6 ) { 
		if( situation < 39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( situation < 33.38 ) { return new Prediction("3", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 45 ) { 
		if( situation < 31 ) { return new Prediction("1", 1, 0); }
		if( situation >= 31 ) { return new Prediction("3", 3, 0); }
	}
		if( situation >= 45 ) { return new Prediction("1", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 28.32 ) { return new Prediction("2", 5, 0); }
	if( situation >= 28.32 ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 2, 0); }
	if( situation < 42.6 ) { 
		if( situation >= 33.98 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.98 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 34.92 ) { 
		if( situation < 29.75 ) { return new Prediction("2", 1, 0); }
	if( situation >= 29.75 ) { 
		if( situation >= 33.27 ) { return new Prediction("1", 4, 2); }
		if( situation < 33.27 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( situation >= 34.92 ) { return new Prediction("1", 4, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 3, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 2, 0); }
	}
	if( "four".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 1, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 1, 0); }
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "high".equals(stability) ) { 
		if( situation >= 31.18 ) { return new Prediction("3", 12, 0); }
	if( situation < 31.18 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("2", 5, 1); }
		if( "medium".equals(budget) ) { return new Prediction("3", 1, 0); }
	}
	}
return null;
}
private Prediction runTree93() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 2, 0); }
	if( "low".equals(budget) ) { 
		if( "medium".equals(stability) ) { return new Prediction("3", 1, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(stability) ) { 
		if( situation >= 40.25 ) { return new Prediction("3", 7, 0); }
	if( situation < 40.25 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 3, 1); }
		if( situation < 33.92 ) { return new Prediction("3", 4, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
	}
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 2, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( situation >= 27.98 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
		if( situation < 39 ) { return new Prediction("1", 2, 0); }
	if( situation >= 39 ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 2, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "medium".equals(budget) ) { 
	if( situation < 44.4 ) { 
	if( situation >= 33.98 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 1, 0); }
		if( situation >= 41.4 ) { return new Prediction("2", 2, 1); }
	}
		if( situation < 33.98 ) { return new Prediction("1", 1, 0); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( situation < 27.98 ) { return new Prediction("2", 4, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 32.55 ) { return new Prediction("1", 3, 0); }
	if( situation >= 32.55 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
	if( situation < 46.8 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 2, 1); }
	}
	}
	}
		if( "one".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "four".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 1, 0); }
	if( "low".equals(stability) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 3, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 7, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 4, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 2, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 4, 0); }
	}
return null;
}
private Prediction runTree94() {
	if( "medium".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 42 ) { return new Prediction("3", 3, 1); }
		if( situation < 42 ) { return new Prediction("1", 3, 0); }
	}
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 3, 0); }
	if( situation >= 30.59 ) { 
		if( situation < 38.75 ) { return new Prediction("3", 1, 0); }
		if( situation >= 38.75 ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "two".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
	if( "high".equals(approval) ) { 
	if( situation >= 43.8 ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 1, 0); }
	}
		if( situation < 43.8 ) { return new Prediction("3", 1, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("2", 1, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.61 ) { return new Prediction("3", 2, 0); }
		if( situation >= 32.61 ) { return new Prediction("2", 2, 0); }
	}
	if( "four".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 3, 0); }
		if( "low".equals(approval) ) { return new Prediction("2", 2, 0); }
	}
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "medium".equals(budget) ) { 
	if( situation < 35.74 ) { 
		if( situation < 22.39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 3, 0); }
	}
	if( situation >= 35.74 ) { 
		if( situation < 44.4 ) { return new Prediction("3", 4, 1); }
		if( situation >= 44.4 ) { return new Prediction("1", 1, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
	if( "two".equals(tier) ) { 
	if( situation < 35.15 ) { 
	if( situation < 31.63 ) { 
		if( situation < 28.1 ) { return new Prediction("2", 1, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 2, 0); }
	}
		if( situation >= 31.63 ) { return new Prediction("2", 2, 0); }
	}
		if( situation >= 35.15 ) { return new Prediction("1", 3, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 4, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.92 ) { return new Prediction("3", 1, 0); }
		if( situation < 33.92 ) { return new Prediction("3", 3, 1); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 9, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 6, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
return null;
}
private Prediction runTree95() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
	if( "low".equals(budget) ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 3, 1); }
		if( situation < 33.92 ) { return new Prediction("3", 3, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( "three".equals(tier) ) { 
	if( "medium".equals(stability) ) { 
		if( situation < 37.8 ) { return new Prediction("1", 3, 0); }
	if( situation >= 37.8 ) { 
		if( situation >= 42.6 ) { return new Prediction("1", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 5, 0); }
	}
	}
	if( "low".equals(stability) ) { 
		if( situation < 22.39 ) { return new Prediction("1", 2, 0); }
	if( situation >= 22.39 ) { 
	if( situation < 44.4 ) { 
		if( situation >= 35.4 ) { return new Prediction("2", 2, 1); }
		if( situation < 35.4 ) { return new Prediction("2", 2, 0); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 7, 0); }
	if( "two".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 8, 0); }
	if( "low".equals(stability) ) { 
	if( situation < 34.92 ) { 
		if( situation < 31.63 ) { return new Prediction("1", 1, 0); }
		if( situation >= 31.63 ) { return new Prediction("1", 2, 1); }
	}
		if( situation >= 34.92 ) { return new Prediction("1", 2, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( "one".equals(tier) ) { 
		if( "medium".equals(stability) ) { return new Prediction("1", 3, 0); }
		if( "low".equals(stability) ) { return new Prediction("2", 1, 0); }
		if( "high".equals(stability) ) { return new Prediction("1", 0, 0); }
	}
	if( "four".equals(tier) ) { 
		if( situation < 39.95 ) { return new Prediction("1", 2, 0); }
		if( situation >= 39.95 ) { return new Prediction("2", 1, 0); }
	}
	}
	if( "low".equals(approval) ) { 
	if( situation >= 36.8 ) { 
		if( "medium".equals(stability) ) { return new Prediction("2", 9, 0); }
		if( "low".equals(stability) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(stability) ) { return new Prediction("3", 2, 0); }
	}
		if( situation < 36.8 ) { return new Prediction("3", 5, 0); }
	}
return null;
}
private Prediction runTree96() {
	if( "medium".equals(stability) ) { 
	if( "high".equals(budget) ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "five".equals(tier) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 4, 0); }
	if( situation < 33.55 ) { 
		if( situation < 30.59 ) { return new Prediction("2", 1, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 4, 0); }
	}
	}
		if( "two".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("2", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("2", 2, 0); }
	}
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 1, 0); }
	if( "high".equals(approval) ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 3, 1); }
	if( situation < 42.6 ) { 
		if( situation < 39 ) { return new Prediction("1", 1, 0); }
		if( situation >= 39 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 41.4 ) { return new Prediction("2", 3, 0); }
	if( situation >= 41.4 ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 1, 0); }
		if( "high".equals(approval) ) { return new Prediction("3", 2, 0); }
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 5, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 29.75 ) { return new Prediction("2", 3, 0); }
		if( situation >= 29.75 ) { return new Prediction("1", 5, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 6, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
	if( "high".equals(stability) ) { 
		if( situation >= 31.18 ) { return new Prediction("3", 19, 0); }
	if( situation < 31.18 ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 3, 1); }
		if( "high".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(approval) ) { return new Prediction("3", 1, 0); }
	}
	}
return null;
}
private Prediction runTree97() {
	if( "medium".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("3", 2, 0); }
	if( "high".equals(approval) ) { 
		if( situation >= 42.6 ) { return new Prediction("3", 1, 0); }
		if( situation < 42.6 ) { return new Prediction("2", 3, 0); }
	}
		if( "low".equals(approval) ) { return new Prediction("2", 1, 0); }
	}
	if( "five".equals(tier) ) { 
	if( situation >= 27.5 ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 1, 0); }
		if( situation < 33.55 ) { return new Prediction("3", 1, 0); }
	}
		if( situation < 27.5 ) { return new Prediction("2", 1, 0); }
	}
	if( "two".equals(tier) ) { 
	if( situation >= 36.23 ) { 
		if( situation < 45 ) { return new Prediction("3", 3, 0); }
		if( situation >= 45 ) { return new Prediction("1", 1, 0); }
	}
	if( situation < 36.23 ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 4, 0); }
		if( "medium".equals(budget) ) { return new Prediction("1", 0, 0); }
	}
	}
	if( "one".equals(tier) ) { 
		if( "medium".equals(approval) ) { return new Prediction("1", 0, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 1, 0); }
	if( "low".equals(approval) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 2, 0); }
		if( situation < 36.8 ) { return new Prediction("3", 2, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
	if( "low".equals(stability) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 22.73 ) { return new Prediction("1", 2, 0); }
	if( situation >= 22.73 ) { 
	if( situation < 44.4 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 3, 0); }
		if( situation >= 41.4 ) { return new Prediction("3", 4, 1); }
	}
	if( situation >= 44.4 ) { 
		if( "medium".equals(approval) ) { return new Prediction("2", 2, 0); }
		if( "high".equals(approval) ) { return new Prediction("1", 1, 0); }
		if( "low".equals(approval) ) { return new Prediction("1", 0, 0); }
	}
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 6, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 28.1 ) { return new Prediction("2", 2, 0); }
		if( situation >= 28.1 ) { return new Prediction("1", 7, 0); }
	}
	if( "one".equals(tier) ) { 
		if( situation < 32.1 ) { return new Prediction("1", 1, 0); }
	if( situation >= 32.1 ) { 
		if( situation >= 36.23 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.23 ) { return new Prediction("2", 2, 0); }
	}
	}
		if( "four".equals(tier) ) { return new Prediction("2", 1, 0); }
	}
		if( "high".equals(stability) ) { return new Prediction("3", 18, 0); }
return null;
}
private Prediction runTree98() {
	if( "medium".equals(approval) ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 33.92 ) { return new Prediction("2", 3, 0); }
	if( situation < 33.92 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 3, 1); }
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 4, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 39.35 ) { 
	if( "three".equals(tier) ) { 
		if( situation >= 27.98 ) { return new Prediction("1", 6, 0); }
	if( situation < 27.98 ) { 
		if( situation < 22.39 ) { return new Prediction("1", 2, 0); }
		if( situation >= 22.39 ) { return new Prediction("2", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("1", 4, 0); }
	if( "one".equals(tier) ) { 
	if( situation >= 29.88 ) { 
		if( situation >= 36.8 ) { return new Prediction("1", 1, 0); }
		if( situation < 36.8 ) { return new Prediction("2", 3, 0); }
	}
		if( situation < 29.88 ) { return new Prediction("1", 1, 0); }
	}
		if( "four".equals(tier) ) { return new Prediction("1", 2, 0); }
	}
	if( situation >= 39.35 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
	if( "low".equals(budget) ) { 
	if( situation >= 42.6 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 2, 1); }
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 3, 0); }
	}
	if( situation < 42.6 ) { 
		if( situation < 40.8 ) { return new Prediction("2", 1, 0); }
		if( situation >= 40.8 ) { return new Prediction("3", 1, 0); }
	}
	}
	if( "medium".equals(budget) ) { 
	if( situation < 44.4 ) { 
		if( situation >= 42 ) { return new Prediction("2", 5, 2); }
		if( situation < 42 ) { return new Prediction("2", 5, 0); }
	}
		if( situation >= 44.4 ) { return new Prediction("1", 2, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 33.55 ) { return new Prediction("2", 6, 0); }
	if( situation < 33.55 ) { 
	if( situation < 31.17 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "five".equals(tier) ) { return new Prediction("2", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("2", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
		if( situation >= 31.17 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 7, 0); }
	}
return null;
}
private Prediction runTree99() {
	if( "medium".equals(approval) ) { 
		if( "high".equals(budget) ) { return new Prediction("2", 1, 0); }
	if( "low".equals(budget) ) { 
	if( "three".equals(tier) ) { 
		if( situation < 28.55 ) { return new Prediction("3", 1, 0); }
	if( situation >= 28.55 ) { 
		if( situation < 42.35 ) { return new Prediction("2", 4, 2); }
		if( situation >= 42.35 ) { return new Prediction("3", 1, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("3", 2, 0); }
		if( "two".equals(tier) ) { return new Prediction("3", 1, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("3", 2, 0); }
	}
		if( "medium".equals(budget) ) { return new Prediction("2", 1, 0); }
	}
	if( "high".equals(approval) ) { 
	if( situation < 39.35 ) { 
	if( situation < 34.92 ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("1", 6, 0); }
	if( "medium".equals(budget) ) { 
	if( situation >= 23.3 ) { 
		if( situation < 28.32 ) { return new Prediction("2", 3, 0); }
	if( situation >= 28.32 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 1, 0); }
		if( "five".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "two".equals(tier) ) { 
		if( situation < 31.63 ) { return new Prediction("1", 1, 0); }
		if( situation >= 31.63 ) { return new Prediction("1", 2, 1); }
	}
		if( "one".equals(tier) ) { return new Prediction("2", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( situation < 23.3 ) { return new Prediction("1", 1, 0); }
	}
	}
		if( situation >= 34.92 ) { return new Prediction("1", 10, 0); }
	}
	if( situation >= 39.35 ) { 
		if( situation < 41.4 ) { return new Prediction("2", 6, 0); }
	if( situation >= 41.4 ) { 
	if( situation >= 42.6 ) { 
	if( "three".equals(tier) ) { 
		if( "high".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "low".equals(budget) ) { return new Prediction("3", 1, 0); }
	if( "medium".equals(budget) ) { 
		if( situation < 44.4 ) { return new Prediction("2", 1, 0); }
		if( situation >= 44.4 ) { return new Prediction("1", 2, 0); }
	}
	}
		if( "five".equals(tier) ) { return new Prediction("1", 1, 0); }
	if( "two".equals(tier) ) { 
		if( situation >= 46.8 ) { return new Prediction("1", 2, 0); }
		if( situation < 46.8 ) { return new Prediction("2", 2, 0); }
	}
		if( "one".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 1, 0); }
	}
		if( situation < 42.6 ) { return new Prediction("3", 1, 0); }
	}
	}
	}
	if( "low".equals(approval) ) { 
	if( "high".equals(budget) ) { 
		if( situation >= 36.8 ) { return new Prediction("2", 5, 0); }
	if( situation < 36.8 ) { 
		if( "three".equals(tier) ) { return new Prediction("1", 0, 0); }
	if( "five".equals(tier) ) { 
		if( situation < 30.59 ) { return new Prediction("2", 3, 0); }
		if( situation >= 30.59 ) { return new Prediction("3", 3, 0); }
	}
		if( "two".equals(tier) ) { return new Prediction("1", 0, 0); }
		if( "one".equals(tier) ) { return new Prediction("3", 3, 0); }
		if( "four".equals(tier) ) { return new Prediction("1", 0, 0); }
	}
	}
		if( "low".equals(budget) ) { return new Prediction("1", 0, 0); }
		if( "medium".equals(budget) ) { return new Prediction("3", 3, 0); }
	}
return null;
}
}
