package root.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import root.entity.DeviceStatus;
import root.repo.DeviceStatusRepo;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class CheckConnectionServiceTest {
    private static final List<String> ips = Collections.singletonList("127.1.1.1");
    @Mock
    private DeviceStatusRepo mockRepo;
    private CheckConnectionService service;

    @Captor
    private ArgumentCaptor<List<DeviceStatus>> captor;

    @Before
    public void init() {
        service = new CheckConnectionService(mockRepo);
    }

    @Test
    public void updateStatus() throws InterruptedException {
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.submit(() -> {
            try {
                this.service.updateStatus(ips);
            } catch (ExecutionException | InterruptedException ignored) {
            }
        });
        this.service.setStop(true);
        service.shutdown();
        service.awaitTermination(1L, TimeUnit.SECONDS);
        verify(mockRepo).saveAll(captor.capture());
        Assert.assertEquals(ips.size(), captor.getValue().size());
    }
}