package org.openmrs.module.atd.web;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.codehaus.jackson.map.ObjectMapper;
import org.openmrs.module.atd.util.ConceptDescriptor;
import org.openmrs.module.atd.util.ImportConceptsUtil;
import org.apache.commons.fileupload.*;
import org.apache.commons.fileupload.servlet.*;
import org.apache.commons.fileupload.disk.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * DWE CHICA-426
 * HttpServlet used to handle uploading the import file, starting the import, and checking progress using AJAX
 */
public class ImportConceptsFromFileServlet extends HttpServlet
{
	private static final long serialVersionUID = 1L;
	protected final Log log = LogFactory.getLog(getClass());
	
	private static final String BEGIN_IMPORT_PARAM = "beginImport";
	private static final String CHECK_PROGRESS_PARAM = "checkProgress";
	private static final String CANCEL_IMPORT_PARAM = "cancelImport";
	private static final String IMPORT_CONCEPTS_ATTRIB = "ImportConceptsThread";
	private static final String IMPORT_STARTED = "importStarted";
	private static final String TOTAL_ROWS_FOUND = "totalRowsFound";
	private static final String CURRENT_ROW = "currentRow";
	private static final String CONCEPTS_CREATED = "conceptsCreated";
	private static final String CONCEPT_ANSWERS_CREATED = "conceptAnswersCreated";
	private static final String IS_COMPLETE = "isComplete";
	private static final String IS_IMPORT_CANCELLED = "isImportCancelled";
	private static final String ERROR_OCCURRED = "errorOccurred";
	private static final String PARSE_ERROR = "fileParseError";
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		HttpSession session = request.getSession();
		Map<String, Object> returnMap = new HashMap<String, Object>();
		ImportConceptsUtil importConcepts = (ImportConceptsUtil)session.getAttribute(IMPORT_CONCEPTS_ATTRIB);
		
		if(request.getParameter(BEGIN_IMPORT_PARAM) != null)
		{
			// This is a bit different than what was originally found in the ImportConceptsFromFileController,
			// but is necessary for ajax
			try
			{
				// Make sure an import isn't already running				
				if(importConcepts != null && importConcepts.getImportStarted() && !importConcepts.getIsImportComplete())
				{
					returnMap.put("alreadyRunning", true);
					writeJSONResponse(response, returnMap);
					return;
				}
				
				RequestContext requestContext = new ServletRequestContext(request);
				if (!ServletFileUpload.isMultipartContent(requestContext))
				{
					returnMap.put("invalidUploadRequest", true);
					log.error("An error occurred starting the import due to an invalid upload request. The request is not a valid multipart/form-data upload request.");
					writeJSONResponse(response, returnMap);
					return;
				}
				
				List<FileItem> items = new ServletFileUpload(new DiskFileItemFactory()).parseRequest(requestContext);

				if(items != null && items.size() == 1)
				{
					FileItem file = items.get(0);
					String filename = file.getName();

					int index = filename.lastIndexOf(".");
					if (index < 0) {						
						returnMap.put("incorrectExtension", true);						
					}

					if(index >= 0) 
					{
						String extension = filename.substring(index + 1, filename.length());
						if (!extension.equalsIgnoreCase("csv")) {
							returnMap.put("incorrectExtension", true);						
						}
						
						if(returnMap.isEmpty()) // No errors have occurred, start the import
						{
							// Determine the number of rows so that the progress bar can be initialized properly
							// This also allows us to make sure an error didn't occur while parsing the file
							List<ConceptDescriptor> list = ImportConceptsUtil.getConcepts(file.getInputStream());
							
							if(list != null && list.size() > 0)
							{
								importConcepts = new ImportConceptsUtil(file.getInputStream());
								Thread thread = new Thread(importConcepts);
								thread.start();

								session.setAttribute(IMPORT_CONCEPTS_ATTRIB, importConcepts);
								returnMap.put(IMPORT_STARTED, true);
								returnMap.put(CURRENT_ROW, importConcepts.getCurrentRow());
								returnMap.put(TOTAL_ROWS_FOUND, list.size());
							}
							else
							{
								returnMap.put(PARSE_ERROR, true);
							}
						}		
					}
				}
			}
			catch(Exception e)
			{
				returnMap.put("serverError", true);
				log.error("An error occurred starting the import.", e);
			}	
		}
		else if(request.getParameter(CANCEL_IMPORT_PARAM) != null)
		{
			if(importConcepts != null && !importConcepts.getIsImportComplete())
			{
				importConcepts.setIsImportCancelled(true);
			}
		}
		else if(request.getParameter(CHECK_PROGRESS_PARAM) != null)
		{
			if(importConcepts != null)
			{
				returnMap.put(TOTAL_ROWS_FOUND, importConcepts.getTotalRowsFound());
				returnMap.put(CURRENT_ROW, importConcepts.getCurrentRow());
				returnMap.put(CONCEPTS_CREATED, importConcepts.getConceptsCreated());
				returnMap.put(CONCEPT_ANSWERS_CREATED, importConcepts.getConceptAnswersCreated());
				returnMap.put(IMPORT_STARTED, importConcepts.getImportStarted());
				returnMap.put(IS_COMPLETE, importConcepts.getIsImportComplete());
				returnMap.put(IS_IMPORT_CANCELLED, importConcepts.getIsImportCancelled());
				returnMap.put(ERROR_OCCURRED, importConcepts.getErrorOccurred());
			}	
		}
		
		writeJSONResponse(response, returnMap);
	}
	
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
	{
		doGet(request, response);
	}
	
	/**
	 * Create a JSON string and write to the response
	 * @param response
	 * @param map
	 * @throws IOException
	 */
	public void writeJSONResponse(HttpServletResponse response, Map<String, Object> map) throws IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		StringWriter writer = new StringWriter();
		mapper.writeValue(writer, map);
		response.getWriter().write(writer.toString());
	}
}