package com.teliasonera.iptv.docker;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.fabric8.docker.api.model.ContainerCreateRequestBuilder;
import io.fabric8.docker.api.model.ContainerCreateResponse;
import io.fabric8.docker.api.model.ContainerExecCreateResponse;
import io.fabric8.docker.api.model.ContainerInspect;
import io.fabric8.docker.api.model.EditableContainerCreateRequest;
import io.fabric8.docker.api.model.ExecConfig;
import io.fabric8.docker.api.model.ExecConfigBuilder;
import io.fabric8.docker.client.Config;
import io.fabric8.docker.client.ConfigBuilder;
import io.fabric8.docker.client.DefaultDockerClient;
import io.fabric8.docker.client.DockerClient;

import static org.assertj.core.api.Assertions.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Exec {

    private static final String CONTAINER_NAME = "my_container";
    private static final String IMAGE_NAME = "mongo:3.2.4";
    private static String EXEC_ID = null;
    private Logger logger = LoggerFactory.getLogger(Containers.class);

    private DockerClient client;

    private static final String NETWORK_NAME = "alfa-net";

    @Before
    public void createClient() {
        Config config = new ConfigBuilder()
                .withDockerUrl("unix:///var/run/docker.sock")
                .build();

        client = new DefaultDockerClient(config);

    }

    @After
    public void closeClient() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testA_create() {
        ContainerCreateRequestBuilder builder = new ContainerCreateRequestBuilder();
        EditableContainerCreateRequest ls = builder.withImage(IMAGE_NAME).withName(CONTAINER_NAME).build();
        ContainerCreateResponse containerCreateResponse = client.container().create(ls);
        logger.info(containerCreateResponse.toString());
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        boolean started = client.container().withName(CONTAINER_NAME).start();
        logger.info("Container started: {}",started);

        ExecConfigBuilder builder2 = new ExecConfigBuilder();
        ExecConfig config = builder2.addToCmd("ls").withDetach(true).build();
        ContainerExecCreateResponse exec = client.container().withName(CONTAINER_NAME).exec(config);
        logger.info("Exec {} created",exec.getId());
        EXEC_ID = exec.getId();
        assertThat(exec.getId()).isNotNull();
        assertThat(exec.getId()).isNotEmpty();

    }

    @Test
    public void testB_start() {
        boolean started = client.exec().withName(EXEC_ID).start();
        logger.info("Exec {} started: {}",EXEC_ID,started);
        assertThat(started).isEqualTo(true);
    }

    @Test
    public void testC_inspect() {
        ContainerInspect inspect = client.exec().withName(EXEC_ID).inspect();
        logger.info(inspect.toString());
    }

    @Test
    public void testX_stop() {
        boolean stopped = client.container().withName(CONTAINER_NAME).stop();
        logger.info("Container {} stopped: {}",CONTAINER_NAME, stopped);
        assertThat(stopped).isEqualTo(true);
    }

    @Test
    public void testZ_remove() {
        boolean removed = client.container().withName(CONTAINER_NAME).remove();
        logger.info("Container {} removed: {}",CONTAINER_NAME,removed);
        assertThat(removed).isEqualTo(true);
    }


}