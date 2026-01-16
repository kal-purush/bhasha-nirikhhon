package hr.tvz.keepthechange.controller;

import hr.tvz.keepthechange.entity.Wallet;
import hr.tvz.keepthechange.service.MonthlyReportService;
import hr.tvz.keepthechange.service.WalletService;
import hr.tvz.keepthechange.util.VendorMediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.security.Principal;

/**
 * Contains mappings related to monthly reports.
 */
@Controller
@RequestMapping("/monthlyReport")
public class MonthlyReportController {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonthlyReportController.class);
    private final MonthlyReportService monthlyReportService;
    private final WalletService walletService;

    public MonthlyReportController(MonthlyReportService monthlyReportService, WalletService walletService) {
        this.monthlyReportService = monthlyReportService;
        this.walletService = walletService;
    }

    @SuppressWarnings("ConstantConditions")
    @GetMapping
    public ResponseEntity<Resource> downloadMonthlyReport(Principal principal) {
        LOGGER.debug("Downloading monthly report");
        return walletService.findFirstByUsername(principal.getName())
                .map(Wallet::getId)
                .filter(monthlyReportService::exists)
                .map(monthlyReportService::get)
                .map(r -> ResponseEntity.ok()
                        .header(HttpHeaders.CONTENT_DISPOSITION, ContentDisposition.attachment().filename(r.getFilename()).build().toString())
                        .contentType(VendorMediaType.APPLICATION_MS_EXCEL_2007)
                        .body(r))
                .orElseGet(() -> ResponseEntity.notFound().build());

    }
}