package fr.abes.bestppn.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.abes.LigneKbartImprime;
import fr.abes.bestppn.dto.PackageKbartDto;
import fr.abes.bestppn.dto.kafka.LigneKbartDto;
import fr.abes.bestppn.dto.kafka.PpnWithDestinationDto;
import fr.abes.bestppn.entity.ExecutionReport;
import fr.abes.bestppn.entity.bacon.Provider;
import fr.abes.bestppn.entity.bacon.ProviderPackage;
import fr.abes.bestppn.exception.*;
import fr.abes.bestppn.kafka.TopicProducer;
import fr.abes.bestppn.repository.bacon.LigneKbartRepository;
import fr.abes.bestppn.repository.bacon.ProviderPackageRepository;
import fr.abes.bestppn.repository.bacon.ProviderRepository;
import fr.abes.bestppn.utils.Utils;
import jakarta.mail.MessagingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClientException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class KbartService {
    private final ObjectMapper mapper;

    private final BestPpnService service;

    private final TopicProducer producer;

    private final EmailService serviceMail;

    private final LogFileService logFileService;

    private final List<LigneKbartDto> kbartToSend = new ArrayList<>();

    private final List<LigneKbartImprime> ppnToCreate = new ArrayList<>();

    private final List<LigneKbartDto> ppnFromKbartToCreate = new ArrayList<>();

    private final PackageKbartDto mailAttachment = new PackageKbartDto();

    private final ProviderPackageRepository providerPackageRepository;

    private final ProviderRepository providerRepository;

    private final LigneKbartRepository ligneKbartRepository;
    private final List<Header> headerList = new ArrayList<>();

    private boolean isOnError = false;

    boolean injectKafka = false;

    private ExecutionReport executionReport = new ExecutionReport();

    private String filename = "";

    private String totalLine = "";

    public KbartService(ObjectMapper mapper, BestPpnService service, TopicProducer producer, EmailService serviceMail, LogFileService logFileService, ProviderPackageRepository providerPackageRepository, ProviderRepository providerRepository, LigneKbartRepository ligneKbartRepository) {
        this.mapper = mapper;
        this.service = service;
        this.producer = producer;
        this.serviceMail = serviceMail;
        this.logFileService = logFileService;
        this.providerPackageRepository = providerPackageRepository;
        this.providerRepository = providerRepository;
        this.ligneKbartRepository = ligneKbartRepository;
    }

    @Transactional
    public void processConsumerRecord(ConsumerRecord<String, String> lignesKbart) throws ExecutionException, InterruptedException, IOException {
        try {
            String currentLine = extractDataFromHeader(lignesKbart.headers().toArray());
            ThreadContext.put("package", (filename + "[line : " + currentLine + "]"));  // Ajoute le numéro de ligne courante au contexte log4j2 pour inscription dans le header kafka

            String nbLine = currentLine + "/" + totalLine;
            String providerName = Utils.extractProvider(filename);
            Optional<Provider> providerOpt = providerRepository.findByProvider(providerName);
            if (lignesKbart.value().equals("OK")) {
                commitDatas(providerOpt, providerName);
            } else {
                handleLigneKbart(lignesKbart.value(), nbLine, providerName);
            }
        } catch (IllegalProviderException e) {
            isOnError = true;
            log.error("Erreur dans les données en entrée, provider incorrect");
            addLineToMailAttachementWithErrorMessage(e.getMessage());
            executionReport.addNbLinesWithInputDataErrors();
        } catch (URISyntaxException | RestClientException | IllegalArgumentException | IOException |
                 IllegalPackageException | IllegalDateException e) {
            isOnError = true;
            log.error(e.getMessage());
            addLineToMailAttachementWithErrorMessage(e.getMessage());
            executionReport.addNbLinesWithInputDataErrors();
        } catch (IllegalPpnException | BestPpnException e) {
            isOnError = true;
            log.error(e.getMessage());
            addLineToMailAttachementWithErrorMessage(e.getMessage());
            executionReport.addNbLinesWithErrorsInBestPPNSearch();
        } catch (MessagingException | ExecutionException | InterruptedException | RuntimeException e) {
            log.error(e.getMessage());
            producer.sendEndOfTraitmentReport(headerList);
            logFileService.createExecutionReport(filename, Integer.parseInt(totalLine), executionReport.getNbLinesOk(), executionReport.getNbLinesWithInputDataErrors(), executionReport.getNbLinesWithErrorsInBestPPNSearch(), injectKafka);
        }
    }

    private void handleLigneKbart(String lignesKbart, String nbLine, String providerName) throws IOException, IllegalPpnException, BestPpnException, URISyntaxException {
        LigneKbartDto ligneFromKafka = mapper.readValue(lignesKbart, LigneKbartDto.class);
        if (ligneFromKafka.isBestPpnEmpty()) {
            log.info("Debut du calcul du bestppn sur la ligne : " + nbLine);
            log.info(ligneFromKafka.toString());
            PpnWithDestinationDto ppnWithDestinationDto = service.getBestPpn(ligneFromKafka, providerName, injectKafka);
            switch (ppnWithDestinationDto.getDestination()) {
                case BEST_PPN_BACON -> {
                    ligneFromKafka.setBestPpn(ppnWithDestinationDto.getPpn());
                    executionReport.addNbBestPpnFind();
                }
                case PRINT_PPN_SUDOC -> {
                    ppnToCreate.add(getLigneKbartImprime(ppnWithDestinationDto, ligneFromKafka));
                }
                case NO_PPN_FOUND_SUDOC -> {
                    if (ligneFromKafka.getPublicationType().equals("monograph")) {
                        ppnFromKbartToCreate.add(ligneFromKafka);
                    }
                }
            }
            //on envoie vers bacon même si on n'a pas trouvé de bestppn
            kbartToSend.add(ligneFromKafka);
        } else {
            log.info("Bestppn déjà existant sur la ligne : " + nbLine + ", le voici : " + ligneFromKafka.getBestPpn());
            kbartToSend.add(ligneFromKafka);
        }
        mailAttachment.addKbartDto(ligneFromKafka);
    }

    private void commitDatas(Optional<Provider> providerOpt, String providerName) throws IllegalPackageException, IllegalDateException, BestPpnException, ExecutionException, InterruptedException, MessagingException, IOException {
        if (!isOnError) {
            ProviderPackage provider = handlerProvider(providerOpt, filename, providerName);

            producer.sendKbart(kbartToSend, provider, filename);
            producer.sendPrintNotice(ppnToCreate, filename);
            producer.sendPpnExNihilo(ppnFromKbartToCreate, provider, filename);
        } else {
            isOnError = false;
        }
        log.info("Nombre de best ppn trouvé : " + executionReport.getNbBestPpnFind() + "/" + totalLine);
        serviceMail.sendMailWithAttachment(filename, mailAttachment);
        producer.sendEndOfTraitmentReport(headerList); // Appel le producer pour l'envoi du message de fin de traitement.
        logFileService.createExecutionReport(filename, Integer.parseInt(totalLine), executionReport.getNbLinesOk(), executionReport.getNbLinesWithInputDataErrors(), executionReport.getNbLinesWithErrorsInBestPPNSearch(), injectKafka);
        kbartToSend.clear();
        ppnToCreate.clear();
        ppnFromKbartToCreate.clear();
        mailAttachment.clearKbartDto();
        executionReport.clear();
    }

    private static LigneKbartImprime getLigneKbartImprime(PpnWithDestinationDto ppnWithDestinationDto, LigneKbartDto ligneFromKafka) {
        LigneKbartImprime ligne = LigneKbartImprime.newBuilder()
                .setPpn(ppnWithDestinationDto.getPpn())
                .setPublicationTitle(ligneFromKafka.getPublicationTitle())
                .setPrintIdentifier(ligneFromKafka.getPrintIdentifier())
                .setOnlineIdentifier(ligneFromKafka.getOnlineIdentifier())
                .setDateFirstIssueOnline(ligneFromKafka.getDateFirstIssueOnline())
                .setNumFirstVolOnline(ligneFromKafka.getNumFirstVolOnline())
                .setNumFirstIssueOnline(ligneFromKafka.getNumFirstIssueOnline())
                .setDateLastIssueOnline(ligneFromKafka.getDateLastIssueOnline())
                .setNumLastVolOnline(ligneFromKafka.getNumLastVolOnline())
                .setNumLastIssueOnline(ligneFromKafka.getNumLastIssueOnline())
                .setTitleUrl(ligneFromKafka.getTitleUrl())
                .setFirstAuthor(ligneFromKafka.getFirstAuthor())
                .setTitleId(ligneFromKafka.getTitleId())
                .setEmbargoInfo(ligneFromKafka.getEmbargoInfo())
                .setCoverageDepth(ligneFromKafka.getCoverageDepth())
                .setNotes(ligneFromKafka.getNotes())
                .setPublisherName(ligneFromKafka.getPublisherName())
                .setPublicationType(ligneFromKafka.getPublicationType())
                .setDateMonographPublishedPrint(ligneFromKafka.getDateMonographPublishedPrint())
                .setDateMonographPublishedOnline(ligneFromKafka.getDateMonographPublishedOnline())
                .setMonographVolume(ligneFromKafka.getMonographVolume())
                .setMonographEdition(ligneFromKafka.getMonographEdition())
                .setFirstEditor(ligneFromKafka.getFirstEditor())
                .setParentPublicationTitleId(ligneFromKafka.getParentPublicationTitleId())
                .setPrecedingPublicationTitleId(ligneFromKafka.getPrecedingPublicationTitleId())
                .setAccessType(ligneFromKafka.getAccessType())
                .build();
        return ligne;
    }

    private String extractDataFromHeader(Header[] headers) {
        String currentLine = "";
        for (Header header : headers) {
            if (header.key().equals("FileName")) {
                filename = new String(header.value());
                headerList.add(header);
                if (filename.contains("_FORCE")) {
                    injectKafka = true;
                }
            } else if (header.key().equals("CurrentLine")) {
                currentLine = new String(header.value());
                headerList.add(header);
            } else if (header.key().equals("TotalLine")) {
                totalLine = new String(header.value());
                executionReport.setNbtotalLines(Integer.parseInt(totalLine));
                headerList.add(header);
            }
        }
        return currentLine;
    }

    private ProviderPackage handlerProvider(Optional<Provider> providerOpt, String filename, String providerName) throws IllegalPackageException, IllegalDateException {
        String packageName = Utils.extractPackageName(filename);
        Date packageDate = Utils.extractDate(filename);
        if (providerOpt.isPresent()) {
            Provider provider = providerOpt.get();

            Optional<ProviderPackage> providerPackageOpt = providerPackageRepository.findByPackageNameAndDatePAndProviderIdtProvider(packageName,packageDate,provider.getIdtProvider());
            if( providerPackageOpt.isPresent()){
                log.info("clear row package : " + providerPackageOpt.get());
                ligneKbartRepository.deleteAllByIdProviderPackage(providerPackageOpt.get().getIdProviderPackage());
                return providerPackageOpt.get();
            } else {
                //pas d'info de package, on le crée
                return providerPackageRepository.save(new ProviderPackage(packageName, packageDate, provider.getIdtProvider(), 'N'));
            }
        } else {
            //pas de provider, ni de package, on les crée tous les deux
            Provider newProvider = new Provider(providerName);
            Provider savedProvider = providerRepository.save(newProvider);
            log.info("Le provider " + savedProvider.getProvider() + " a été créé.");
            ProviderPackage providerPackage = new ProviderPackage(packageName, packageDate, savedProvider.getIdtProvider(), 'N');
            return providerPackageRepository.save(providerPackage);
        }
    }

    private void addLineToMailAttachementWithErrorMessage(String messageError) {
        LigneKbartDto ligneVide = new LigneKbartDto();
        ligneVide.setErrorType(messageError);
        mailAttachment.addKbartDto(ligneVide);
    }

}