package org.teradata.dblp;

/**************************************************
 * XML Parser for DBLP data
 *
 * Martin Kaufmann (martin.kaufmann@teradata.com)
 **************************************************/

import java.io.*;
import javax.xml.stream.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class DblpParser {

    // files
    public static final String XML_FILE = "dblp.xml";
    //public static final String XML_FILE = "dblp_small.xml";
    public static final String INPUT_FOLDER = "input";
    public static final String OUTPUT_FOLDER = "output";

    // XML config
    private HashSet<String> PUBLICATION_TYPES = new HashSet<>(Arrays.asList("article", "inproceedings", "proceedings", "book", "incollection", "phdthesis", "mastersthesis"));
    private String VENUE_TYPE = "proceedings";
    private String PHD_THESIS_TYPE = "phdthesis";
    private String BOOK_TYPE = "book";
    private String WWW_TYPE = "www";

    // private variables
    private HashMap<String,ArrayList<Publication>> resultAuthorToPublications = new HashMap<String,ArrayList<Publication>>();
    private HashMap<String,ArrayList<Publication>> resultAuthorToPhdThesis = new HashMap<String,ArrayList<Publication>>();
    private HashMap<String,ArrayList<Publication>> resultAuthorToBook = new HashMap<String,ArrayList<Publication>>();
    private HashMap<String,String> resultKeyToVenue = new HashMap<String,String>();
    private HashMap<String,String> resultIdentialName = new HashMap<String,String>(); // map alternative names to unified name

    /*
     * Constructor
     */
    public DblpParser(String inputXmlFile) {
        // read input file
        XMLStreamReader reader;
        try {
            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            InputStream in = new FileInputStream(INPUT_FOLDER + "/" + XML_FILE);
            reader = inputFactory.createXMLStreamReader(in);
        } catch(Exception e) {
            throw new IllegalArgumentException("Cannot open XML file: " + e.getMessage());
        }

        // parse XML
        try {
            parseDblp(reader);
        } catch(Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Cannot parse XML file: " + e.getMessage());
        }
    }

    /*
     * Parse DBLP data set
     */
    public void writeCSV() {
        ArrayList<String> output = new ArrayList<String>(100);

        // compute author IDs
        System.out.println("Compute author IDs...");
        int thisAuthorId = 1;
        HashMap<String,Integer> authorIdMap = new HashMap<>();
        HashMap<Integer,String> idAuthorMap = new HashMap<>();
        for(String authorName: resultAuthorToPublications.keySet()) {
            authorIdMap.put(authorName, thisAuthorId);
            idAuthorMap.put(thisAuthorId, authorName);
            thisAuthorId++;
        }

        //private HashMap<String,ArrayList<Publication>> resultAuthorToPublications = new HashMap<String,ArrayList<Publication>>();
        //private HashMap<String,ArrayList<Publication>> resultAuthorToPhdThesis = new HashMap<String,ArrayList<Publication>>();
        //private HashMap<String,ArrayList<Publication>> resultAuthorToBook = new HashMap<String,ArrayList<Publication>>();

        // titleAuthorVenue
        try {
            String fileName = "01titleAuthorVenue.txt";
            System.out.println("Writing CSV output file: " + fileName);
            FileWriter fw = new FileWriter(OUTPUT_FOLDER + "/" + fileName);

            output.clear();
            output.add("#publicationTitle");
            output.add("authorId");
            output.add("authorName");
            output.add("venueName");
            output.add("publicationDate");
            output.add("publicationKey");
            fw.write(String.join("|", output) + "\n");

            for(String author: resultAuthorToPublications.keySet()) {
                for(Publication publication: resultAuthorToPublications.get(author)) {
                    String venueKey = publication.getCrossref();
                    String venueName = null;
                    if(venueKey != null) {
                        venueName = resultKeyToVenue.get(venueKey);
                    }
                    if(venueName == null) {
                        venueName = "";
                    }
                    output.clear();
                    output.add(printValue(publication.getTitle()));
                    output.add(printValue(authorIdMap.get(author).toString()));
                    output.add(printValue(author));
                    output.add(printValue(venueName));
                    output.add(printValue(publication.getDate()));
                    output.add(printValue(publication.getKey()));
                    fw.write(String.join("|", output) + "\n");
                }
            }
            fw.close();
        } catch(IOException e) {
            throw new IllegalArgumentException("Cannot write output file: " + e.getMessage());
        }

        // authorPublications
        writePublicationType("02authorPublications.txt", resultAuthorToPublications, authorIdMap);

        // authorVenues
        try {
            String fileName = "03authorVenues.txt";
            System.out.println("Writing CSV output file: " + fileName);
            FileWriter fw = new FileWriter(OUTPUT_FOLDER + "/" + fileName);

            output.clear();
            output.add("authorId");
            output.add("authorName");
            output.add("venueName1");
            output.add("venueName2");
            output.add("...");
            fw.write(String.join("|", output) + "\n");

            for(String author: resultAuthorToPublications.keySet()) {
                StringBuffer thisRecord = new StringBuffer (author);
                output.clear();
                output.add(printValue(authorIdMap.get(author).toString()));
                output.add(printValue(author));
                int count = 0;
                for(Publication publication: resultAuthorToPublications.get(author)) {
                    String venueKey = publication.getCrossref();
                    String venueName = null;
                    if(venueKey != null) {
                        venueName = resultKeyToVenue.get(venueKey);
                    }
                    if(venueName != null) {
                        output.add(printValue(venueName));
                    }
                }
                fw.write(String.join("|", output) + "\n");
            }
            fw.close();
        } catch(IOException e) {
            throw new IllegalArgumentException("Cannot write output file: " + e.getMessage());
        }

        // authorPhdThesis
        writePublicationType("04authorPhdThesis.txt", resultAuthorToPhdThesis, authorIdMap);

        // authorBook
        writePublicationType("05authorBook.txt", resultAuthorToBook, authorIdMap);

        // authorId
        try {
            String fileName = "06authorDictionary.txt";
            System.out.println("Writing CSV output file: " + fileName);
            FileWriter fw = new FileWriter(OUTPUT_FOLDER + "/" + fileName);

            output.clear();
            output.add("#authorId");
            output.add("authorName");
            fw.write(String.join("|", output) + "\n");

            for(Integer authorId: idAuthorMap.keySet()) {
                output.clear();
                output.add(printValue(authorId.toString()));
                output.add(printValue(idAuthorMap.get(authorId)));
                fw.write(String.join("|", output) + "\n");
            }
            fw.close();
        } catch(IOException e) {
            throw new IllegalArgumentException("Cannot write output file: " + e.getMessage());
        }
    }

    /*
     * Print value
     */
    private String printValue(String value) {
        if(value == null) {
            return "";
        } else {
            return value.replaceAll("\\|", "");
        }
    }

    /*
     * Write output for a given publication type
     */
    private void writePublicationType(String fileName, HashMap<String,ArrayList<Publication>> result, HashMap<String,Integer> authorIdMap) {
        System.out.println("Writing CSV output file: " + fileName);
        try {
            ArrayList<String> output = new ArrayList<String>(100);
            FileWriter fw = new FileWriter(OUTPUT_FOLDER + "/" + fileName);

            output.clear();
            output.add("#authorId");
            output.add("#authorName");
            output.add("publicationTitle1");
            output.add("publicationTitle2");
            output.add("...");
            fw.write(String.join("|", output) + "\n");

            for(String author: result.keySet()) {
                StringBuffer thisRecord = new StringBuffer (author);
                output.clear();
                output.add(printValue(author));
                for(Publication publication: result.get(author)) {
                    output.add(printValue(publication.getTitle()));
                }
                fw.write(String.join("|", output) + "\n");
            }
            fw.close();
        } catch(IOException e) {
            throw new IllegalArgumentException("Cannot write output file: " + e.getMessage());
        }
    }

    /*
     * Parse DBLP data set
     */
    private void parseDblp(final XMLStreamReader xmlStreamReader) throws XMLStreamException {

        int count = 0;
        ArrayList<String> authors = null;
        String title = null;
        String publicationKey = null;
        String publicationDate = null;
        String crossref = null;
        boolean isPublication = false;
        boolean isWww = false;
        String venueKey = null;
        ArrayList<String> identialAuthors = null;
        while(xmlStreamReader.hasNext()) {

            // Get integer value of current event
            int xmlEvent = xmlStreamReader.next();

            // Process start element.
            if (xmlEvent == XMLStreamConstants.START_ELEMENT) {
                String localName = xmlStreamReader.getLocalName();

                // Venue: get name of conference/journal etc. where publication has been published
                if(VENUE_TYPE.equals(localName)) {
                    title = null;

                    // get key of venue
                    for (int i=0; i < xmlStreamReader.getAttributeCount(); i++) {
                        if(xmlStreamReader.getAttributeLocalName(i).equals("key")) {
                            venueKey = xmlStreamReader.getAttributeValue(i);
                        }
                    }
                }

                // WWW: identify duplicate authors
                if(WWW_TYPE.equals(localName)) {
                    isWww = true;
                    identialAuthors = new ArrayList<String>();
                }
                else if(isWww && "author".equals(localName)) {
                    identialAuthors.add(xmlStreamReader.getElementText());
                }

                // Get data about the publication
                if(PUBLICATION_TYPES.contains(localName)) {
                    //count++;
                    isPublication = true;
                    authors = new ArrayList<String>();
                    title = null;
                    publicationKey = null;
                    publicationDate = null;

                    // get date of publication
                    for (int i=0; i < xmlStreamReader.getAttributeCount(); i++) {
                        if(xmlStreamReader.getAttributeLocalName(i).equals("key")) {
                            publicationKey = xmlStreamReader.getAttributeValue(i);
                        }
                        else if(xmlStreamReader.getAttributeLocalName(i).equals("mdate")) {
                            publicationDate = xmlStreamReader.getAttributeValue(i);
                        }
                    }
                }
                else if(isPublication && "title".equals(localName)) {
                    title = readElementBody(xmlStreamReader);
                    //System.out.println("title="+title);
                }
                else if(isPublication && "crossref".equals(localName)) {
                    crossref = readElementBody(xmlStreamReader);
                }
                else if(isPublication && "author".equals(localName)) {
                    authors.add(xmlStreamReader.getElementText());
                }
            }

            // Process end element.
            else if (xmlEvent == XMLStreamConstants.END_ELEMENT) {
                String localName = xmlStreamReader.getLocalName();

                if(VENUE_TYPE.equals(localName)) {
                    //System.out.println("venueKey="+venueKey + ", title=" + title);
                    if(venueKey != null && title != null) {
                        resultKeyToVenue.put(venueKey, title);
                    }
                    venueKey = null;
                }

                if(WWW_TYPE.equals(localName)) {
                    //System.out.println("venueKey="+venueKey + ", title=" + title);
                    if(identialAuthors.size() > 1) {
                        //throw new IllegalArgumentException("identical authors=" + identialAuthors.size());
                        String unifiedName = identialAuthors.get(0);
                        for(int i=1; i<identialAuthors.size(); i++) {
                            resultIdentialName.put(identialAuthors.get(i), unifiedName);
                            //System.out.println("alternative name=" + identialAuthors.get(i) + ", unifiedName=" + unifiedName);
                        }
                    }

                    identialAuthors = new ArrayList<String>();
                    isWww = false;
                }

                if(PUBLICATION_TYPES.contains(localName)) {
                    if(title != null) {
                        for (String author : authors) {
                            addPublicationToAuthor(author, publicationKey, title, crossref, publicationDate, localName);
                        }
                    }
                    count++;
                    isPublication = false;
                }
            }
        }

        // unify publications if people with alternative names
        System.out.println("\nUnify publications of people with alternative names...");
        int countAlternativeNames = 0;
        for(String alternativeName: resultIdentialName.keySet()) {
            mergePublications(alternativeName, resultIdentialName.get(alternativeName), resultAuthorToPublications);
            mergePublications(alternativeName, resultIdentialName.get(alternativeName), resultAuthorToPhdThesis);
            mergePublications(alternativeName, resultIdentialName.get(alternativeName), resultAuthorToBook);
            countAlternativeNames++;
        }
        System.out.println("# alternative names: " + countAlternativeNames);

        System.out.println("\nSummary:");
        System.out.println("# Publications: " + count);
    }

    /*
     * Merge publications of two alternative names
     */
    private void mergePublications(String alternativeName, String unifiedName, HashMap<String,ArrayList<Publication>> publications) {
        ArrayList<Publication> alternativePublications = publications.get(alternativeName);
        ArrayList<Publication> unifiedPublications = publications.get(unifiedName);

        // handle different cases
        if(alternativePublications != null && alternativePublications.size() > 0) {
            if(unifiedPublications != null) {
                unifiedPublications.addAll(alternativePublications);
            } else {
                publications.put(unifiedName, alternativePublications);
            }
            publications.remove(alternativeName);
        }
    }

    /*
     * Add publication object to author
     */
    private void addPublicationToAuthor(String author, String key, String title, String crossref, String publicationDate, String publicationType) {
        // general publications
        addPublicationToAuthor(author, key, title, crossref, publicationDate, resultAuthorToPublications);

        // phd thesis
        if(publicationType.equals(PHD_THESIS_TYPE)) {
            addPublicationToAuthor(author, key, title, crossref, publicationDate, resultAuthorToPhdThesis);
        }

        // books
        if(publicationType.equals(BOOK_TYPE)) {
            addPublicationToAuthor(author, key, title, crossref, publicationDate, resultAuthorToBook);
        }
    }

    /*
     * Add publications of a given type
     */
    private void addPublicationToAuthor(String author, String key, String title, String crossref, String publicationDate, HashMap<String,ArrayList<Publication>> authorToPublications) {
        ArrayList<Publication> titles = authorToPublications.get(author);
        if(titles == null) {
            titles = new ArrayList<Publication>();
            authorToPublications.put(author, titles);
        }
        titles.add(new Publication(key, title, crossref, publicationDate));
    }

    /*
     * Get String representation of a tag
     */
    private String readElementBody(XMLStreamReader xmlStreamReader)
            throws XMLStreamException {
        StringWriter buf = new StringWriter(1024);

        int depth = 0;
        while (xmlStreamReader.hasNext()) {
            // peek event
            int xmlEvent = xmlStreamReader.next();

            if (xmlEvent == XMLStreamConstants.CHARACTERS) {
                buf.append(xmlStreamReader.getText());
            }
            else if (xmlEvent == XMLStreamConstants.START_ELEMENT) {
                ++depth;
            }
            else if (xmlEvent == XMLStreamConstants.END_ELEMENT) {
                --depth;
                // reached END_ELEMENT tag?
                // break loop, leave event in stream
                if (depth < 0) {
                    break;
                }
            }

        }

        return buf.getBuffer().toString();
    }

    /*
     * Main method
     */
    public static void main(String[] args)  {

        System.out.println("Parsing DBLP XML file...");
        long startTime = System.currentTimeMillis();
        DblpParser dblp = new DblpParser(XML_FILE);
        long endTime = System.currentTimeMillis();
        long runTime = (endTime - startTime) / 1000;
        System.out.println("\nRuntime for parsing DBLP dataset: " + runTime + " sec");

        System.out.println("\nWriting CSV files...");
        dblp.writeCSV();
        System.out.println("Writing CSV files done.");
    }
}