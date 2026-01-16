package PlanQ.PlanQ.downloadLogs;

import PlanQ.PlanQ.Member.Member;
import PlanQ.PlanQ.report.Report;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class DownloadService {
    private final DownloadLogsRepository downloadLogsRepository;

    public void downLoad(Member member, Report report){
        DownloadLogs downloadLogs = downloadLogsRepository.findByMemberAndReport(member, report);
        if(downloadLogs != null){
            downloadLogs.updateDate();
            return;
        }
        DownloadLogs newDownloadLogs = DownloadLogs.builder()
                .member(member)
                .report(report)
                .build();
        downloadLogsRepository.save(newDownloadLogs);
    }
}