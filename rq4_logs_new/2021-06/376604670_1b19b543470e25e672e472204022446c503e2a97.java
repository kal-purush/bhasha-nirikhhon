package service;

import org.springframework.stereotype.Service;
import repository.*;

@Service
public class MSSQLEventService {
    private final ApplicationNameRepository applicationNameRepository;
    private final ComputerNameRepository computerNameRepository;
    private final ContextOneCRepository contextOneCRepository;
    private final ProcessNameRepository processNameRepository;
    private final SqlRepository sqlRepository;
    private final UserEventRepository userEventRepository;

    public MSSQLEventService(ApplicationNameRepository applicationNameRepository,
                             ComputerNameRepository computerNameRepository, ContextOneCRepository contextOneCRepository,
                             ProcessNameRepository processNameRepository, SqlRepository sqlRepository,
                             UserEventRepository userEventRepository) {
        this.applicationNameRepository = applicationNameRepository;
        this.computerNameRepository = computerNameRepository;
        this.contextOneCRepository = contextOneCRepository;
        this.processNameRepository = processNameRepository;
        this.sqlRepository = sqlRepository;
        this.userEventRepository = userEventRepository;
    }



}