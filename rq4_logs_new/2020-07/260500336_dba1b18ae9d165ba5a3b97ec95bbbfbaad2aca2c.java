package world.podo.emergency.domain.notice;

import lombok.RequiredArgsConstructor;
import world.podo.emergency.domain.DomainService;

@DomainService
@RequiredArgsConstructor
public class NoticeSynchronizationService {
    private final NoticeFactory noticeFactory;
    private final NoticeRepository noticeRepository;

    public Notice synchronize(NoticeFetchDetailValue noticeFetchDetailValue) {
        if (noticeFetchDetailValue == null) {
            return null;
        }
        return noticeRepository.findByProviderNoticeId(noticeFetchDetailValue.getId())
                               .map(notice -> notice.update(
                                       noticeFetchDetailValue.getId(),
                                       noticeFetchDetailValue.getTitle(),
                                       noticeFetchDetailValue.getTextContent(),
                                       noticeFetchDetailValue.getHtmlContent(),
                                       noticeFetchDetailValue.getWrittenDate()
                               ))
                               .orElseGet(() -> noticeFactory.create(
                                       noticeFetchDetailValue.getId(),
                                       noticeFetchDetailValue.getTitle(),
                                       noticeFetchDetailValue.getTextContent(),
                                       noticeFetchDetailValue.getHtmlContent(),
                                       noticeFetchDetailValue.getWrittenDate()
                               ));
    }
}