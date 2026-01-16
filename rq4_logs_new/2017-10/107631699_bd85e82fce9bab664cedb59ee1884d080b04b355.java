package cn.edu.nju.cs.itrace4.gitProject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import cn.edu.nju.cs.itrace4.core.dataset.TextDataset;
import cn.edu.nju.cs.itrace4.demo.exp.project.Infinispan;
import cn.edu.nju.cs.itrace4.demo.exp.project.Itrust;
import cn.edu.nju.cs.itrace4.demo.exp.project.Maven;
import cn.edu.nju.cs.itrace4.demo.exp.project.Project;
import cn.edu.nju.cs.itrace4.relation.RelationInfo;

public class AdjustParameter {
	private Map<String, Double> irPvalueMap = new ConcurrentHashMap<String, Double>();
	private Map<String, Double> udPvalueMap = new ConcurrentHashMap<String, Double>();

	public void lookForParameter() throws InterruptedException, IOException, ClassNotFoundException {
		System.setProperty("routerLen", 6 + "");
		Project[] projects = { new Itrust(), new Maven(), new Infinispan() };
		String[] models = { "cn.edu.nju.cs.itrace4.core.ir.VSM", "cn.edu.nju.cs.itrace4.core.ir.JSD",
				"cn.edu.nju.cs.itrace4.core.ir.LSI" };
		List<Thread> threadList = new ArrayList<Thread>();
		AtomicInteger ai = new AtomicInteger();

		for (double callThreshold = 0.2; callThreshold < 0.99; callThreshold += 0.1) {
			for (double dataThreshold = 0.2; dataThreshold < 0.99; dataThreshold += 0.1) {
				for (Project project : projects) {
					for (String model : models) {
						TextDataset textDataset = new TextDataset(project.getUcPath(), project.getClassDirPath(),
								project.getRtmClassPath());
						FileInputStream fis = new FileInputStream(project.getClass_RelationInfoPath());
						ObjectInputStream ois = new ObjectInputStream(fis);
						RelationInfo ri = (RelationInfo) ois.readObject();
						ois.close();
						ai.getAndIncrement();
						//System.out.println(ai);
						Executor executor = new Executor(callThreshold, dataThreshold, project, model, irPvalueMap,
								udPvalueMap);
						executor.init(textDataset,
								ri/* ,class_relation,class_relationForO,class_relationForAllDependencies */);
						Thread cur = new Thread(executor);
						cur.start();
						threadList.add(cur);
						if (threadList.size() % 8 == 0) {
							for (Thread thread : threadList) {
								thread.join();
							}
							threadList.clear();
						}
					}
				}
			}
		}

		for (Thread thread : threadList) {
			thread.join();
		}

		try {
			storeExcel();
		} catch (Exception e) {
			e.printStackTrace();
		}
		findBestParameter(0.0588);
	}

	public void findBestParameter(double threshold) {
		Map<String, List<Double>> irMap = reArrange(irPvalueMap);
		Map<String, List<Double>> udMap = reArrange(udPvalueMap);
		for (String str : irMap.keySet()) {
			List<Double> irList = irMap.get(str);
			List<Double> udList = udMap.get(str);
			boolean allLessThanThreshold = true;
			for (double val : irList) {
				if (val > threshold) {
					allLessThanThreshold = false;
					break;
				}
			}
			for (double val : udList) {
				if (val > threshold) {
					allLessThanThreshold = false;
					break;
				}
			}
			if (allLessThanThreshold) {
				System.out.println(str);
			}
		}
	}

	/**
	 * @author zzf
	 * @date 2017/10/21
	 * @description find best parameter pair.
	 */
	private Map<String, List<Double>> reArrange(Map<String, Double> pvalueMap) {
		Map<String, List<Double>> map = new HashMap<String, List<Double>>();
		for (String key : pvalueMap.keySet()) {
			String callDataIden = getCallDataIden(key);
			if (!map.containsKey(callDataIden)) {
				map.put(callDataIden, new LinkedList<Double>());
			}
			map.get(callDataIden).add(pvalueMap.get(key));
		}
		return map;
	}

	private String getCallDataIden(String str) {
		int start = str.indexOf('-');
		int end = str.lastIndexOf('-');
		return str.substring(start + 1, end);
	}

	private void storeExcel() throws FileNotFoundException, IOException {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(new File("irPvalueMap.out")));
		out.writeObject(irPvalueMap);
		out = new ObjectOutputStream(new FileOutputStream(new File("udPvalueMap.out")));
		out.writeObject(udPvalueMap);
		out.close();
	}

	public static void main(String[] args) throws InterruptedException, ClassNotFoundException, IOException {
		AdjustParameter tool = new AdjustParameter();
		tool.lookForParameter();
	}
}