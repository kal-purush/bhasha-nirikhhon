import java.io.*;
import java.util.*;
import weka.core.*;

//TODO: Testing instances using this tree.

public class ID3Generator
	{

	int debugLevel = 0;

	TreeNode<Id3Node> generateID3Tree(Instances instances, ArrayList<Attribute> attributes, int m, TreeNode<Id3Node> parent)
		{
		if(debugLevel >= 1)
			{
			System.out.println("\r\nGEN - instances : " + instances.numInstances() + " Attrs : " + instances.numAttributes() + " M: " + m + " parent: " + parent);
			}

		TreeNode<Id3Node> root = new TreeNode<Id3Node>();
		root.data = new Id3Node(instances, attributes);

		// truncate now - base cases - all same class
		if(instances.attributeStats(instances.classIndex()).distinctCount == 1)
			{
			// NOTE : works only when classifying, not when doing regression.
			root.data.makeLeaf(instances.firstInstance().stringValue(instances.classAttribute()));
			if(debugLevel >= 1)
				System.out.println("IsLeaf? " + root.data.isLeaf);
			}
		// no instances in this node
		else if(instances.numInstances() == 0)
			{
			root.data.makeLeaf(instances.classAttribute().value(0));
			// TODO : will this still work if there are no instances ? ie,
			// header info exists? EDIT : Apparently, yes
			if(debugLevel >= 1)
				System.out.println("IsLeaf? " + root.data.isLeaf);
			}
		// no attributes left, or this.numInstances < m
		else if(attributes.size() == 0 || instances.numInstances() < m)
			{
			if(root.data.numPos > instances.numInstances() / 2)
				{
				String classVal = instances.classAttribute().value(1);
				root.data.makeLeaf(classVal);
				}
			else
				{
				String classVal = instances.classAttribute().value(0);
				root.data.makeLeaf(classVal);
				}
			if(debugLevel >= 1)
				System.out.println("IsLeaf? " + root.data.isLeaf);
			}
		// create more children
		else
			{
			// Choose best attr
			double maxInfoGain = -1;
			Attribute maxInfoGainAttr = null;
			double maxInfoGainSplit = Double.NEGATIVE_INFINITY;
			List<String> maxGainNominalValues = new LinkedList<String>();

			for (Attribute attr : attributes)
				{
				double infoGain = -1;
				double bestSplit = Double.NEGATIVE_INFINITY;

				if(attr.isNominal())
					{
					infoGain = getNominalInfoGain(attr, instances);
					}
				else if(attr.isNumeric())
					{
					double bestSplitInfoGain = -1;
					bestSplit = Double.NEGATIVE_INFINITY;
					List<Double> candidateSplits = getCandidateSplits(attr, instances);

					for (Double candidateSplit : candidateSplits)
						{
						double splitInfoGain = getRealValuedInfoGain(attr, instances, candidateSplit);
						if(splitInfoGain >= bestSplitInfoGain)
							{
							bestSplitInfoGain = splitInfoGain;
							bestSplit = candidateSplit;
							}
						}
					infoGain = bestSplitInfoGain;
					if(debugLevel >= 1)
						System.out.println("Numeric IG: Real attr: " + attr.name() + " thresh: " + bestSplit + " Best IG: " + bestSplitInfoGain);
					}
				if(infoGain > maxInfoGain)
					{
					maxInfoGain = infoGain;
					maxInfoGainAttr = attr;
					if(attr.isNumeric())
						{
						maxInfoGainSplit = bestSplit;
						}
					else
						{
						maxInfoGainSplit = Double.NEGATIVE_INFINITY;
						}
					}
				}

			if(debugLevel >= 1)
				{
				System.out.println("Best Attr: " + maxInfoGainAttr.name() + " Nominal? " + maxInfoGainAttr.isNominal() + " Thresh: " + maxInfoGainSplit);
				}

			// split on best attr, recurse
			ArrayList<Attribute> subAttrs = (ArrayList<Attribute>) attributes.clone();
			if(maxInfoGainAttr.isNominal())
				{
				subAttrs.remove(maxInfoGainAttr);
				List<String> possibleVals = Collections.list(maxInfoGainAttr.enumerateValues());
				for (String possibleVal : possibleVals)
					{
					root.data.isLeaf = false;
					Instances subInstances = getInstancesSubsetOnNominalCondition(maxInfoGainAttr, possibleVal, instances);
					if(debugLevel >= 1)
						{
						System.out.println("IsLeaf? " + root.data.isLeaf);
						System.out.println("Recursing.." + root.toString());
						}
					TreeNode<Id3Node> child = generateID3Tree(subInstances, subAttrs, m, root);
					if(debugLevel >= 1)
						System.out.println("Exitting Recursing.." + root.toString());
					child.data.parentAttr = maxInfoGainAttr;
					child.data.parentAttrVal = possibleVal;
					child.data.parentAttrCompOp = "=";
					root.addChild(child);

					}
				}
			else if(maxInfoGainAttr.isNominal() == false)
				{
				Instances[] subInstances = getInstancesSubsetsOnRealThreshold(maxInfoGainAttr, maxInfoGainSplit, instances);
				for (int i = 0;i < subInstances.length;i++)
					{
					if(subInstances[i].numInstances() > 0)
						{
						root.data.isLeaf = false;
						if(debugLevel >= 1)
							{
							System.out.println("IsLeaf? " + root.data.isLeaf);
							System.out.println("Recursing.." + root.toString());
							}
						TreeNode<Id3Node> child = generateID3Tree(subInstances[i], subAttrs, m, root);
						if(debugLevel >= 1)
							System.out.println("Exitting Recursing.." + root.toString());
						child.data.parentAttr = maxInfoGainAttr;
						child.data.parentAttrVal = maxInfoGainSplit;
						child.data.parentAttrCompOp = (i == 0)? "<=" : ">";
						root.addChild(child);
						}
					}
				}
			}

		return root;
		}

	//@formatter:off
//	iff all instances are positive return node with +
//	iff all instances are neg return node with -
//	iff instances.count < minNumInsts
//	iff attrs are empty, return single node tree with most common instance class
	
//	Else
//		A <- best attr 
//			foreach attr in attributes, compute IG
//				if attr is real, compute IG for each candidate split
//		Foreach possible val for nominal A, (or,)
//		Foreach child when real A is split by midpoint of A in instances
//			childInstances <- Instances.select(A.val compOp decisionVal )
//			if(childInstances.size == 0)
//				in instances, pick popular class, add leaf node
//			else
//				addChildNode(instances, attributes-A)
//	Return root
	//@formatter:on

	public List<Double> getCandidateSplits(Attribute a, Instances instances)
		{
		ArrayList<Double> candidateSplits = new ArrayList<Double>();

		Instances insts = new Instances(instances);

		insts.sort(a);
		double[] allVals = insts.attributeToDoubleArray(a.index());

		LinkedHashMap<Double, String> attrValAndClass = new LinkedHashMap<Double, String>();

		// store attrVal and class - both if both.
		for (int i = 0;i < insts.numInstances();i++)
			{
			Instance inst1 = insts.instance(i);
			double attrVal1 = inst1.value(a);
			String classVal1 = inst1.stringValue(insts.classIndex());

			if(!attrValAndClass.containsKey(attrVal1))
				{
				attrValAndClass.put(attrVal1, classVal1);
				}
			else
				{
				if(classVal1 != attrValAndClass.get(attrVal1))
					{
					attrValAndClass.replace(attrVal1, "both");
					}
				}
			}

		// Iterate over each val of attrVal and associated class
		Map.Entry<Double, String> prev = null;
		for (Map.Entry<Double, String> attrVal_class : attrValAndClass.entrySet())
			{
			if(prev != null)
				{
				if(prev.getValue() == "both" || prev.getValue() != attrVal_class.getValue())
					{
					candidateSplits.add((prev.getKey() + attrVal_class.getKey()) / 2);
					}
				}
			// if(attrVal_class.getValue() == "both")
			// {
			// candidateSplits.add(attrVal_class.getKey());
			// }
			prev = attrVal_class;
			}

		if(debugLevel >= 2)
			{
			System.out.println("CandidateSplits - Attr: " + a.name() + " numInsts: " + insts.numInstances());
			System.out.println("Attr Vals : " + Arrays.toString(allVals));
			System.out.println(candidateSplits.toString());
			}
		return candidateSplits;

		}

	public double getRealValuedInfoGain(Attribute a, Instances insts, double thresh)
		{
		double ig = getEntropy(insts);

		double entVal[] = new double[2];
		int numInClass[] = new int[2];
		Instances[] subInsts = new Instances[2];

		subInsts[0] = new Instances(insts, insts.numInstances());
		subInsts[1] = new Instances(insts, insts.numInstances());

		int numInsts = insts.numInstances();
		if(numInsts == 0)
			{
			return 0;
			}

		for (int i = 0;i < numInsts;i++)
			{
			Instance inst = insts.instance(i);
			double instAval = inst.value(a);

			if(instAval <= thresh)
				{
				numInClass[0]++;
				subInsts[0].add(inst);
				}
			else
				{
				numInClass[1]++;
				subInsts[1].add(inst);
				}
			}

		entVal[0] = this.getEntropy(subInsts[0]);
		entVal[1] = this.getEntropy(subInsts[1]);

		ig -= (double) ((double) subInsts[0].numInstances() / (double) numInsts) * entVal[0];
		ig -= (double) ((double) subInsts[1].numInstances() / (double) numInsts) * entVal[1];

		if(debugLevel >= 2)
			{
			System.out.println("RealIG - Attr: " + a.name() + "\tThresh :" + thresh + "\tIg :" + ig);
			if(ig == Double.NaN)
				{
				System.out.println("\t\tIG is NaN");
				}
			}

		return ig;
		}

	public double getNominalInfoGain(Attribute a, Instances insts)
		{
		double ig = 0.0;
		if(insts.numInstances() == 0)
			{
			return ig;
			}

		Instances[] subInsts = new Instances[insts.numDistinctValues(a)];

		HashMap<String, Integer> attValToIndex = new HashMap<String, Integer>();

		// add to subinsts
		int numUniqVals = 0;
		int numInsts = insts.numInstances();
		for (int i = 0;i < numInsts;i++)
			{
			Instance inst = insts.instance(i);
			String attVal = inst.stringValue(a);

			if(!attValToIndex.containsKey(attVal))
				{
				attValToIndex.put(attVal, numUniqVals++);
				}

			Integer subIndex = attValToIndex.get(attVal);
			if(subInsts[subIndex] == null)
				{
				subInsts[subIndex] = new Instances(insts, insts.numInstances());
				}
			subInsts[subIndex].add(inst);
			}

		// Compute sub-entropies, final IG
		double totEntropy = getEntropy(insts);
		double[] entVal = new double[insts.numDistinctValues(a)];
		ig = totEntropy;

		for (int i = 0;i < numUniqVals;i++)
			{
			entVal[i] = getEntropy(subInsts[i]);
			ig -= ((double) ((double) subInsts[i].numInstances() / (double) insts.numInstances()) * entVal[i]);
			}

		if(debugLevel >= 1)
			{
			System.out.println("Nominal IG: Attr: " + a.name() + " numInsts: " + insts.numInstances() + " numUniqVals: " + numUniqVals + " IG: " + ig);
			if(ig == Double.NaN)
				{
				System.out.println("\t\tIG is NaN");
				}
			}

		return ig;
		}

	public double getEntropy(Instances insts)
		{
		if(insts.numInstances() == 0)
			{
			return 0.0;
			}
		double entropy = -1.0;

		int numInsts = insts.numInstances();
		int classInd = insts.classIndex();
		double numPos = 0, numNeg = 0;

		for (int i = 0;i < numInsts;i++)
			{
			double cls = insts.instance(i).value(classInd);
			if((int) cls == 1)
				{
				numPos++;
				}
			else if((int) cls == 0)
				{
				numNeg++;
				}
			else
				{
				System.out.println("\r\nEXCEPTION!! CRASH AND BURN - Class value not in 0, 1");
				}
			}

		double totNum = numPos + numNeg;

		if(numPos == 0 || numNeg == 0)
			{
			return 0.0;
			}

		entropy = (-1 * getLogBase2(numPos / totNum) * (numPos / totNum)) + (-1 * getLogBase2(numNeg / totNum) * (numNeg / totNum));

		if(entropy == Double.NaN)
			{
			System.out.println("\t\tEntropy is NaN");
			}
		return entropy;
		}

	double getLogBase2(double num)
		{
		return Math.log(num) / Math.log(2.0);
		}

	private Instances[] getInstancesSubsetsOnRealThreshold(Attribute a, double thresh, Instances instances)
		{
		Instances[] subInsts = new Instances[2];
		subInsts[0] = new Instances(instances, instances.numInstances()); // <=
		subInsts[1] = new Instances(instances, instances.numInstances()); // >

		int numInsts = instances.numInstances();
		for (int i = 0;i < numInsts;i++)
			{
			Instance inst = instances.instance(i);
			if(inst.value(a) <= thresh)
				{
				subInsts[0].add(inst);
				}
			else
				{
				subInsts[1].add(inst);
				}
			}
		return subInsts;
		}

	public Instances getInstancesSubsetOnNominalCondition(Attribute a, String aVal, Instances insts)
		{
		Instances subInsts = new Instances(insts, insts.numInstances());
		int numInsts = insts.numInstances();
		for (int i = 0;i < numInsts;i++)
			{
			Instance inst = insts.instance(i);
			String attVal = inst.stringValue(a);

			if(aVal.equalsIgnoreCase(attVal))
				{
				subInsts.add(inst);
				}
			}
		return subInsts;
		}

	// Recursive Print function
	public void recPrintID3Tree(TreeNode<Id3Node> root, FileWriter outFileWriter, int level)
		{
		try
			{
			if(level != 0)
				{
				outFileWriter.write("\r\n");
				System.out.print("\r\n");
				}
			for (int i = 1;i < level;i++)
				{
				outFileWriter.write("|\t");
				System.out.print("|\t");
				}
			if(level != 0)
				{
				outFileWriter.write(root.data.toString());
				System.out.print(root.data.toString());
				}
			for (TreeNode<Id3Node> child : root.children)
				{
				recPrintID3Tree(child, outFileWriter, level + 1);
				}
			outFileWriter.flush();
			// if(level == 0)
			// {
			// System.out.println();
			// }
			}
		catch(IOException e)
			{
			e.printStackTrace();
			}
		// System.out.println("Finished writing..");
		}

	}