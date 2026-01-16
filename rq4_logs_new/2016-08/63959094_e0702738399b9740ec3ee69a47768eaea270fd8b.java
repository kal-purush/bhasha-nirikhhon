package Cheiron.Datatarget;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.vfs2.FileNotFolderException;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;

public class CSVHead extends Datatarget {

	protected String path = new String();
	protected String filters = new String();
	protected String feature = new String();
	protected String featval = new String();
	protected String onlytop = new String();

	private void writeBlock(JCas cas, String inc) throws Exception {
		Iterator<JCas> it = cas.getViewIterator();
		Class<? extends Annotation> cls = Class.forName(inc).asSubclass(Annotation.class);
		Type type = JCasUtil.getAnnotationType(cas, cls);
		Feature feat = type.getFeatureByBaseName(featval);
		PrintWriter writer = new PrintWriter(
				new File(new File(path), "head-" + inc.substring(inc.lastIndexOf(".") + 1) + ".csv"));

		writer.println("# Head entries for type " + inc);
		writer.println("# pid, first, second, third, ...");

		while (it.hasNext()) {
			JCas view = it.next();

			if (view.getViewName().equals("_InitialView"))
				continue;

			Collection<? extends Annotation> collection = JCasUtil.select(view, cls);
			Annotation[] array = collection.toArray(new Annotation[collection.size()]);
			List<String> line = new ArrayList<String>();
			line.add(view.getViewName());

			for (int i = 0; i < array.length && i < Integer.parseInt(onlytop); i++)
				line.add(array[i].getFeatureValueAsString(feat));

			writer.println(String.join(", ", line));
		}

		writer.close();
	}

	@Override
	public void write(Map<String, JCas> data) throws Exception {
		File directory = new File(path);

		if (!directory.isDirectory())
			throw new FileNotFolderException(directory.getAbsolutePath());

		if (feature.isEmpty() || featval.isEmpty())
			return;

		for (Entry<String, JCas> e : data.entrySet())
			if (filters.isEmpty() || filters.contains(e.getKey().substring(0, e.getKey().indexOf("-"))))
				for (String inc : feature.split(", "))
					writeBlock(e.getValue(), inc);
	}

}