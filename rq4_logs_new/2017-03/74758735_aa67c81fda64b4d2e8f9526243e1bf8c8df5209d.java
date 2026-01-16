package it.tn.rivadelgarda.comune.gda.docer.keys;

/**
 * 
 * @author mirco
 *
 */
public interface MetadatoDocer {

	public final static String TYPE_ID_KEY = "TYPE_ID";
	public final static String DOCNAME_KEY = "DOCNAME";
	public final static String ABSTRACT_KEY = "ABSTRACT";
	public final static String COD_ENTE_KEY = "COD_ENTE";
	public final static String COD_AOO_KEY = "COD_AOO";
	public final static String TIPO_COMPONENTE_KEY = "TIPO_COMPONENTE";
	public final static String DOCNUM_KEY = "DOCNUM";
	public final static String STATO_ARCHIVISTICO_KEY = "STATO_ARCHIVISTICO";
	public final static String STATO_BUSINESS_KEY = "STATO_BUSINESS";
	public final static String CREATION_DATE_KEY = "CREATION_DATE";
	public final static String ARCHIVE_TYPE_KEY = "ARCHIVE_TYPE";
	public final static String DOC_URL_KEY = "DOC_URL";
	public final static String APP_VERSANTE_KEY = "APP_VERSANTE";
	public final static String DOC_HASH_KEY = "DOC_HASH";
	public final static String DOCNUM_RECORD_KEY = "DOCNUM_RECORD";
	public final static String UD_VERSION_KEY = "UD_VERSION";
	public final static String EXTERNAL_ID_KEY = "EXTERNAL_ID";
	
    public final static String FOLDER_NAME_KEY = "FOLDER_NAME";
    public final static String FOLDER_OWNER_KEY = "FOLDER_OWNER";
    public final static String DES_FOLDER_KEY = "DES_FOLDER";
    public final static String ENABLED_KEY = "ENABLED";
    public final static String PARENT_FOLDER_NAME_KEY = "PARENT_FOLDER_NAME";
    public final static String FOLDER_ID_KEY = "FOLDER_ID";
    public final static String PARENT_FOLDER_ID_KEY = "PARENT_FOLDER_ID";

    public final static String TYPE_ID_DOCUMENTO = "DOCUMENTO";
    
    public final static String TIPO_COMPONENTE_PRINCIPALE = "PRINCIPALE";
    public final static String TIPO_COMPONENTE_ALLEGATO = "ALLEGATO";
    public final static String TIPO_COMPONENTE_ANNESSO = "ANNESSO";
    public final static String TIPO_COMPONENTE_ANNOTAZIONE = "ANNOTAZIONE";
    
    public final static String ARCHIVE_TYPE_ARCHIVE = "ARCHIVE";
    public final static String ARCHIVE_TYPE_URL = "URL";

	public final static String SORT_ASC = "ASC";
	public final static String SORT_DESC = "DESC";
	
    String getValue();
}