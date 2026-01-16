package test;

	import org.apache.log4j.Logger;   

	import java.io.*;
	import java.net.*;
	import java.util.*;

public class ValidateStream {
	    final static Logger logger = Logger.getLogger(ValidateStream.class);

		public ValidateStream()
	    {

		}

	    //    ------------------------------------------------------------------------------------

	    public  List<String> readManifset(URL url) throws Exception
	     {
	         if(logger.isDebugEnabled())
	         logger.info("Start Call function readManifset(url)");

	         List<String> listOfParts=new ArrayList<String>();
	         String blockPart = "";
	         BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()));

	    try
	    {
	      String line = br.readLine();
	      while (line != null)
	       {
	        line=line.replaceAll("\\s+","");// remove space

	        if(line.equals("**"))
	        {
	        listOfParts.add(blockPart);
	        blockPart = "" ;
	        }
	        else
	        {
	        if(!validateURL(line))
	        throw new Exception("*** In valid manifset formate.");

	        blockPart+=line+";";
	        }
	        line = br.readLine();
	       }
	        listOfParts.add(blockPart);
	    }
	    catch (Exception e)
	    {
	        if(logger.isDebugEnabled())
	        logger.error("Error multipart.Parsing.readManifset:" + e);

	        throw e;

	    }
	    finally
	    {
	        br.close();
	    }

	         if(logger.isDebugEnabled())
	         logger.info("End Call function readManifset(url)");

			return listOfParts ;
		}

	    // ****************************************************** validURL Function ********
	    private boolean validateURL(String url)
	    {
	        URL u = null;
	        try
	        {
	            u = new URL(url);
	        } catch (MalformedURLException e)
	        {
	            System.err.println("Error in multipart.Parsing.validateURL:"+e);
	            return false;
	        }
	        try
	        {
	            u.toURI();
	        }
	        catch (URISyntaxException e)
	        {
	            System.err.println("Error in multipart.Parsing.validateURL"+e);
	            return false;
	        }

	        return true;
	    }

	}