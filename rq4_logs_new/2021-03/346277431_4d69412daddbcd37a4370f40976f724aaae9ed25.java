package hr.tvz.keepthechange.service;

import hr.tvz.keepthechange.entity.Transaction;
import hr.tvz.keepthechange.util.JxlsUtil;
import org.jxls.common.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.YearMonth;
import java.util.List;

@Service
public class MonthlyReportService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonthlyReportService.class);
    public static final String MONTHLY_REPORT_FOLDER = "monthlyReport";
    public static final String MONTHLY_REPORT_FILENAME_PATTERN = "monthly-report-{walletId}-{yearMonth}.xlsx";
    private final JxlsService jxlsService;
    private final FileManagerService fileManagerService;

    public MonthlyReportService(JxlsService jxlsService, FileManagerService fileManagerService) {
        this.jxlsService = jxlsService;
        this.fileManagerService = fileManagerService;
    }

    public boolean generate(YearMonth yearMonth, Long walletId, List<Transaction> transactions) {
        LOGGER.debug("Generating monthly report for wallet {} and yearMonth {}", walletId, yearMonth);
        Context context = new Context();
        context.putVar("yearMonth", yearMonth);
        context.putVar("transactions", transactions);
        context.putVar("locale", LocaleContextHolder.getLocale());

        try (InputStream is = new ClassPathResource(JxlsUtil.MONTHLY_REPORT_TEMPLATE).getInputStream()) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                jxlsService.transform(is, baos, context);
                boolean saved = fileManagerService.saveFile(generateFilePath(yearMonth, walletId), baos.toByteArray());
                if (saved) {
                    LOGGER.debug("Monthly report for wallet {} and yearMonth {} generated", walletId, yearMonth);
                }
                return saved;
            }
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
    }

    private String generateFilePath(YearMonth yearMonth, Long walletId) {
        return MONTHLY_REPORT_FOLDER +
                "/" +
                walletId +
                "/" +
                generateFilename(yearMonth, walletId);
    }

    private String generateFilename(YearMonth yearMonth, Long walletId) {
        return MONTHLY_REPORT_FILENAME_PATTERN.replace("{walletId}", walletId.toString())
                .replace("{yearMonth}", yearMonth.toString());
    }
}