package lv.ailab.tezaurs;

import java.io.*;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;
import org.w3c.dom.*;

/**
 * @author Lauma, Valters Rostoks
 */
public class DictionaryDic2Xml
{
	public static String inputDataPath = "./dati/dic/";
	public static String outputDataPath = "./dati/xml/";

	protected Document doc;
	protected Element fullEntries;
	protected Element refEntries;
	protected BufferedReader currentDicIn;
	protected PrintWriter log;

	protected String line, pline, VR;
	protected int NS;
	protected boolean parcelts;

	public DictionaryDic2Xml()
	throws IOException, ParserConfigurationException
	{
		try
		{
			log = new PrintWriter(new OutputStreamWriter(new FileOutputStream(
					outputDataPath + "dic2xml-log.txt"), "Windows-1257"), true);

			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			doc = builder.newDocument();

			fullEntries = doc.createElement("tezaurs");
			refEntries = doc.createElement("sliktie");

			fullEntries
					.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
			fullEntries
					.setAttribute("xsi:noNamespaceSchemaLocation", "tezaurs.xsd");
		} catch (IOException|ParserConfigurationException e)
		{
			e.printStackTrace(log);
			throw e;
		}

		// TODO tikt no šitā vaļā
		doc.appendChild(fullEntries);

		currentDicIn = null;

	}

	//Atgriež "true", ja vārdam ir nozīmju grupa (tas ir "labais" vārds)
	protected boolean mkVR(Element s) throws Exception
	{
		boolean labais = false;
		VR = line.substring(3);

		String RU = null;
		String IN = null;
		String GR = null;

		do
		{
			line = currentDicIn.readLine();

			if (line.indexOf("IN ") == 0) IN = line.substring(3);
			if (line.indexOf("RU ") == 0) RU = line.substring(3);

			if (line.indexOf("GR ") == 0)
			{
				GR = line.substring(3);
				if (GR.charAt(GR.length() - 1) == '-')
					GR = (GR.substring(0, GR.length() - 1)).trim();
			}
		} while ((line.indexOf("GR ") == 0) || (line
				.indexOf("IN ") == 0) || (line.indexOf("RU ") == 0));

		if (IN != null) s.setAttribute("i", IN);

		Element vf = doc.createElement("vf");
		if (RU != null) vf.setAttribute("ru", RU);
		vf.setTextContent(VR);

		Element gram = doc.createElement("gram");
		if (GR != null) gram.setTextContent(GR);

		Element v = doc.createElement("v");
		v.appendChild(vf);
		if (GR != null)
		{
			v.appendChild(gram);
		}
		//else {System.err.println("Missing GR: " + VR);}
		s.appendChild(v);

		NS = 0;
		if (line.indexOf("NS ") == 0) line = currentDicIn.readLine();

		Element g_n = null;
		while (line.indexOf("NO ") == 0)
		{
			if (g_n == null) g_n = doc.createElement("g_n");
			mkNO(g_n);
		}

		if (g_n != null)
		{
			s.appendChild(g_n);
			labais = true;
		}

		Element g_fraz = null;
		Element g_de = null;

		if (line.indexOf("FS ") == 0) line = currentDicIn.readLine();

		while (line.indexOf("FR ") == 0)
		{
			if (g_fraz == null) g_fraz = doc.createElement("g_fraz");
			mkFR(g_fraz);
		}

		if (line.indexOf("DS ") == 0) line = currentDicIn.readLine();
		if (line == null) return labais;

		while (line.indexOf("DE ") == 0)
		{
			if (g_de == null) g_de = doc.createElement("g_de");
			mkDE(g_de);
			if (line == null) return labais;
		}

		if (g_fraz != null) s.appendChild(g_fraz);
		if (g_de != null) s.appendChild(g_de);
		if (line.indexOf("DN ") == 0) mkDN(s);
		if (line.indexOf("CD ") == 0) mkDN(s);
		if (line.indexOf("LI ") == 0) mkLI(s);

		return labais;
	}

	protected void mkNO(Element g_n) throws Exception
	{
		NS++;

		Element n = doc.createElement("n");
		n.setAttribute("nr", String.valueOf(NS));
		Element d = doc.createElement("d");
		Element t = doc.createElement("t");

		t.setTextContent((line.substring(3)).trim());
		d.appendChild(t);

		line = currentDicIn.readLine();
		if (line.indexOf("NG ") == 0) mkNG(n);

		n.appendChild(d);

		Element g_piem = null;
		Element g_an = null;

		int is_bad = 0;

		while ((line.indexOf("AN ") == 0) || (line.indexOf("PI ") == 0) || (line
				.indexOf("PG ") == 0))
		{
			while (line.indexOf("AN ") == 0)
			{
				if (g_an == null) g_an = doc.createElement("g_an");
				mkAN(g_an);
			}

			if (line.indexOf("PG ") == 0) mkPG(null);

			while (line.indexOf("PI ") == 0)
			{
				if (g_piem == null) g_piem = doc.createElement("g_piem");
				mkPI(g_piem);
			}
		}

		if (g_piem != null) n.appendChild(g_piem);
		if (g_an != null) n.appendChild(g_an);
		g_n.appendChild(n);
	}


	protected void mkNG(Element n) throws Exception
	{
		Element gram = doc.createElement("gram");
		line = (line.substring(3)).trim();
		if (line.charAt(line.length() - 1) == '-')
			line = (line.substring(0, line.length() - 1)).trim();

		gram.setTextContent(line);
		n.appendChild(gram);
		line = currentDicIn.readLine();
	}

	protected void mkPN(Element piem) throws Exception
	{
		Element n = doc.createElement("n");
		Element d = doc.createElement("d");
		Element t = doc.createElement("t");

		t.setTextContent((line.substring(3)).trim());
		d.appendChild(t);
		n.appendChild(d);
		piem.appendChild(n);
		line = currentDicIn.readLine();
	}


	//--------------------------------------------------------------------------------------------------
	protected void mkPG(Element el) throws Exception
	{
		boolean apst = false;
		if (!parcelts)
		{

			if (el == null)
			{
				pline = line;
				line = currentDicIn.readLine();
				apst = true;
				log.println("!!! " + pline + " " + VR);
			}
			if (apst)
			{
				parcelts = true;
				if (line.indexOf("PN ") == 0)
				{
					String temp = line;
					line = pline;
					pline = temp;
					mkPG(el);
				}
				return;
			}
		}
		parcelts = false;

		if (el == null)
		{
			log.println("BAD!!! " + line + " " + VR);
			return;
		}

		Element a = doc.createElement("gram");
		line = (line.substring(3)).trim();
		if (line.charAt(line.length() - 1) == '-')
			line = (line.substring(0, line.length() - 1)).trim();
		a.setTextContent(line);
		el.appendChild(a);
		if (pline != null)
		{
			line = pline;
			pline = null;
		} else line = currentDicIn.readLine();
	}

	protected void mkAN(Element el) throws Exception
	{
		Element nel = doc.createElement("n");
		Element d = doc.createElement("d");
		Element t = doc.createElement("t");
		t.setTextContent((line.substring(3)).trim());
		d.appendChild(t);
		line = currentDicIn.readLine();
		if (line.indexOf("AG ") == 0) mkNG(nel);
		nel.appendChild(d);
		if (line.indexOf("PG ") == 0) mkPG(null);
		Element g_piem = null;
		while (line.indexOf("PI ") == 0)
		{
			if (g_piem == null) {g_piem = doc.createElement("g_piem");}
			mkPI(g_piem);
		}
		if (g_piem != null) nel.appendChild(g_piem);
		el.appendChild(nel);
	}

	protected void mkPI(Element el) throws Exception
	{
		Element b = doc.createElement("piem");
		Element a = doc.createElement("t");
		line = (line.substring(3)).trim();
		if (line.charAt(line.length() - 1) == '-')
			line = (line.substring(0, line.length() - 1)).trim();
		a.setTextContent(line);
		b.appendChild(a);
		if (pline != null)
		{
			line = pline;
			pline = null;
		} else line = currentDicIn.readLine();
		if (line.indexOf("PG ") == 0) mkPG(b);
		while (line.indexOf("PN ") == 0) mkPN(b);
		el.appendChild(b);
	}

	protected void mkFR(Element el) throws Exception
	{
		Element fel = doc.createElement("fraz");
		Element t = doc.createElement("t");
		line = (line.substring(3)).trim();
		if (line.charAt(line.length() - 1) == '-')
			line = (line.substring(0, line.length() - 1)).trim();
		t.setTextContent(line);
		fel.appendChild(t);
		line = currentDicIn.readLine();
		if (line.indexOf("FG ") == 0) mkNG(fel);
		if (line.indexOf("FP ") == 0) mkFP(fel, false);
		while (line.indexOf("FN ") == 0) mkFN(fel);
		el.appendChild(fel);
	}

	protected void mkFN(Element el) throws Exception
	{
		Element nel = doc.createElement("n");
		Element d = doc.createElement("d");
		Element t = doc.createElement("t");
		t.setTextContent((line.substring(3)).trim());
		d.appendChild(t);
		line = currentDicIn.readLine();
		nel.appendChild(d);
		while (line.indexOf("FP ") == 0) mkFP(nel, true);
		el.appendChild(nel);
	}

	protected void mkFP(Element el, boolean labs)
	throws Exception
	{
		if (labs)
		{
			Element b = doc.createElement("piem");
			Element a = doc.createElement("t");
			a.setTextContent((line.substring(3)).trim());
			b.appendChild(a);
			el.appendChild(b);
		} else
		{
			Element nel = doc.createElement("n");
			Element d = doc.createElement("d");
			Element t = doc.createElement("t");
			d.appendChild(t);
			nel.appendChild(d);
			Element b = doc.createElement("piem");
			Element a = doc.createElement("t");
			a.setTextContent((line.substring(3)).trim());
			b.appendChild(a);
			nel.appendChild(b);
			el.appendChild(nel);
		}
		line = currentDicIn.readLine();
	}

	protected void mkDE(Element el)
	throws Exception
	{
		Element fel = doc.createElement("de");
		Element a = doc.createElement("v");
		Element t = doc.createElement("vf");
		t.setTextContent((line.substring(3)).trim());
		a.appendChild(t);
		line = currentDicIn.readLine();
		if (line.indexOf("DG ") == 0) mkNG(a);
		fel.appendChild(a);
		while (line.indexOf("DP ") == 0) mkDP(fel);
		el.appendChild(fel);
	}

	protected void mkDP(Element el)
	throws Exception
	{
		Element a = doc.createElement("piem");
		Element b = doc.createElement("t");
		b.setTextContent((line.substring(3)).trim());
		a.appendChild(b);
		line = currentDicIn.readLine();
		if (line.indexOf("DA ") == 0) mkNG(a);
		if (line.indexOf("DD ") == 0) mkPN(a);
		el.appendChild(a);
	}
//--------------------------------------------------------------------------------------------------


	protected void mkDN(Element s) throws Exception
	{
		Element ref = doc.createElement("ref");
		ref.setTextContent((line.substring(3)).trim());
		s.appendChild(ref);
		line = currentDicIn.readLine();
	}

	protected void mkLI(Element s) throws Exception
	{
		Element avots = doc.createElement("avots");
		avots.setTextContent((line.substring(3)).trim());
		s.appendChild(avots);
		line = currentDicIn.readLine();
	}

	public static void main(String[] args)
	throws IOException, ParserConfigurationException, TransformerException
	{
		File folder = new File(inputDataPath);
		if (!folder.exists())
		{
			System.out.println(
					"Ups! Nevar atrast ieejas datu mapi \"" + inputDataPath + "\"!");
			return;
		}
		File dicFolder = new File(outputDataPath);
		if (!dicFolder.exists()) dicFolder.mkdirs();

		DictionaryDic2Xml transformator = new DictionaryDic2Xml();

		File[] listOfFiles = folder.listFiles();
		for (File f : listOfFiles)
		{
			String fileName = f.getName();
			if (!fileName.endsWith(".dic"))
				continue;
			try
			{
				BufferedReader in = new BufferedReader(new FileReader(inputDataPath + fileName));
				transformator.addDicToXml(in);
				System.out.println(fileName + " [Pabeigts]");
			} catch (Exception e)
			{
				e.printStackTrace(System.err);
				e.printStackTrace(transformator.log);
				System.out.println(fileName + " [Problēma]");
			}

		}
		transformator.writeEverything();
		System.out.println("Viss pabeigts!");
	}

	public void addDicToXml(BufferedReader reader)
	throws Exception
	{
		currentDicIn = reader;
		while (true)
		{
			do
			{
				line = currentDicIn.readLine();
				if (line == null) break;
				line = line.trim();
			} while (line.indexOf("VR ") != 0);

			if (line == null) break;

			Element s = doc.createElement("s");
			if (mkVR(s)) fullEntries.appendChild(s);
			else refEntries.appendChild(s);

			if (line == null) break;
			if (line.length() > 0)
			{
				log.println(line + "  " + VR);
			}
		}
		currentDicIn = null;
	}

	public void writeEverything()
	throws TransformerException, IOException
	{
		try
		{
			TransformerFactory transFactory = TransformerFactory.newInstance();
			Transformer transformer = transFactory.newTransformer();
			PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputDataPath + "entries.xml"), "UTF8")));
			Source src = new DOMSource(doc);
			Result target = new StreamResult(out);

			transformer.transform(src, target);

			out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputDataPath + "references.xml"), "UTF8")));
			doc.removeChild(fullEntries);
			doc.appendChild(refEntries);
			src = new DOMSource(doc);
			target = new StreamResult(out);

			transformer.transform(src, target);

			log.close();
		}
		catch (TransformerException|IOException e)
		{
			e.printStackTrace(log);
			throw e;
		}
	}
}