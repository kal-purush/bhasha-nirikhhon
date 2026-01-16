package uk.gov.hmcts.reform.pip.channel.management.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.gov.hmcts.reform.pip.channel.management.errorhandling.exceptions.ProcessingException;
import uk.gov.hmcts.reform.pip.channel.management.services.filegeneration.FileConverter;
import uk.gov.hmcts.reform.pip.channel.management.services.helpers.DateHelper;
import uk.gov.hmcts.reform.pip.channel.management.services.helpers.LanguageResourceHelper;
import uk.gov.hmcts.reform.pip.model.location.Location;
import uk.gov.hmcts.reform.pip.model.publication.Artefact;
import uk.gov.hmcts.reform.pip.model.publication.Language;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@SuppressWarnings("PMD.PreserveStackTrace")
public class PublicationFileGenerationService {
    private static final int MAX_FILE_SIZE = 2_000_000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DataManagementService dataManagementService;
    private final ListConversionFactory listConversionFactory;

    @Value("${pdf.font}")
    private String pdfFont;

    @Autowired
    public PublicationFileGenerationService(DataManagementService dataManagementService,
                                            ListConversionFactory listConversionFactory) {
        this.dataManagementService = dataManagementService;
        this.listConversionFactory = listConversionFactory;
    }

    public static String maskDataSourceName(String provenance) {
        return "SNL".equals(provenance) ? "ListAssist" : provenance;
    }

    /**
     * Generate publication files for a given artefact.
     *
     * @param artefactId The artefact ID to generate the files for.
     * @return all generated files (English PDF + Welsh PDF + Excel).
     * @throws ProcessingException error.
     */
    public Triple<byte[], byte[], byte[]> generate(UUID artefactId) {
        String rawJson = dataManagementService.getArtefactJsonBlob(artefactId);
        Artefact artefact = dataManagementService.getArtefact(artefactId);
        Location location = dataManagementService.getLocation(artefact.getLocationId());
        JsonNode topLevelNode;

        try {
            topLevelNode = MAPPER.readTree(rawJson);
            FileConverter fileConverter = listConversionFactory.getFileConverter(artefact.getListType());

            if (fileConverter == null) {
                log.error("Failed to find converter for list type");
                return Triple.of(new byte[0], new byte[0], new byte[0]);
            }

            byte[] excel = fileConverter.convertToExcel(topLevelNode, artefact.getListType());

            // Generate the English and/or Welsh PDFs and store in Azure blob storage
            Pair<byte[], byte[]> pdfs = generatePdfs(topLevelNode, artefact, location);

            // Return in the order of English PDF, Welsh PDF and Excel
            return Triple.of(pdfs.getLeft(), pdfs.getRight(), excel);

        } catch (IOException ex) {
            throw new ProcessingException(String.format("Failed to generate files for artefact id %s", artefactId));
        }
    }

    /**
     * Generate the English and/or Welsh PDF for a given artefact.
     *
     * @param topLevelNode The data node.
     * @param artefact The artefact.
     * @param location The location.
     * @return a byte array of the generated pdf.
     * @throws IOException error.
     */
    private Pair<byte[], byte[]> generatePdfs(JsonNode topLevelNode, Artefact artefact, Location location)
        throws IOException {
        Language language = artefact.getLanguage();

        if (artefact.getListType().hasAdditionalPdf() && language != Language.ENGLISH) {
            byte[] englishPdf = generatePdf(topLevelNode, artefact, location, Language.ENGLISH, true);
            if (englishPdf.length > MAX_FILE_SIZE) {
                englishPdf = generatePdf(topLevelNode, artefact, location, Language.ENGLISH, false);
            }

            byte[] welshPdf = generatePdf(topLevelNode, artefact, location, Language.WELSH, true);
            if (welshPdf.length > MAX_FILE_SIZE) {
                welshPdf = generatePdf(topLevelNode, artefact, location, Language.WELSH, false);
            }

            return Pair.of(englishPdf, welshPdf);
        }

        byte[] pdf = generatePdf(topLevelNode, artefact, location, language, true);
        if (pdf.length > MAX_FILE_SIZE) {
            pdf = generatePdf(topLevelNode, artefact, location, language, false);
        }

        return Pair.of(pdf, new byte[0]);
    }

    /**
     * Generate the PDF for a given artefact.
     *
     * @param topLevelNode The data node.
     * @param artefact The artefact.
     * @param location The location where the artefact is uploaded to.
     * @param language The language of the artefact.
     * @param accessibility If the pdf should be accessibility generated.
     * @return a byte array of the generated pdf.
     * @throws IOException Throw if error generating.
     */
    private byte[] generatePdf(JsonNode topLevelNode, Artefact artefact, Location location, Language language,
                               boolean accessibility) throws IOException {
        Map<String, Object> languageResource = LanguageResourceHelper.getLanguageResources(
            artefact.getListType(), language);
        String html = listConversionFactory.getFileConverter(artefact.getListType())
            .convert(topLevelNode, buildArtefactMetadata(artefact, location, language), languageResource);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            PdfRendererBuilder builder = new PdfRendererBuilder();
            builder.useFastMode()
                .usePdfAConformance(PdfRendererBuilder.PdfAConformance.PDFA_1_A);

            File file = new File(pdfFont);
            if (file.exists()) {
                builder.useFont(file, "openSans");
            } else {
                builder.useFont(new File(Thread.currentThread().getContextClassLoader()
                                             .getResource("openSans.ttf").getFile()), "openSans");
            }
            builder.usePdfUaAccessbility(accessibility);
            builder.withHtmlContent(html, null)
                .toStream(baos)
                .run();
            return baos.toByteArray();
        }
    }

    private Map<String, String> buildArtefactMetadata(Artefact artefact, Location location, Language language) {
        String locationName = (language == Language.ENGLISH) ? location.getName() : location.getWelshName();
        String provenance = maskDataSourceName(artefact.getProvenance());
        return Map.of(
            "contentDate", DateHelper.formatLocalDateTimeToBst(artefact.getContentDate()),
            "provenance", provenance,
            "locationName", locationName,
            "region", String.join(", ", location.getRegion()),
            "regionName", String.join(", ", location.getRegion()),
            "language", language.toString(),
            "listType", artefact.getListType().name()
        );
    }

}