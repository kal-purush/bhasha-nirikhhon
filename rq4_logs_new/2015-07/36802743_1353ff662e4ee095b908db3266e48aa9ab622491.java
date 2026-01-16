/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.bioboxes.bioboxmesossheduler.sheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Volume;

/**
 * Example scheduler to launch Docker containers.
 */
public class MesosScheduler implements Scheduler {

    /**
     * Logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(MesosScheduler.class);

    /**
     * Task ID generator.
     */
    private final AtomicInteger taskIDGenerator = new AtomicInteger();

    private final List<Protos.TaskInfo> pendingTasks = new ArrayList<>();
    private final Map<String, Protos.TaskInfo> runningTasks = new HashMap<>();
    private final Map<String, Protos.TaskInfo> finishedTasks = new HashMap<>();
    private final Map<String, Protos.TaskInfo> failedTasks = new HashMap<>();

    private Protos.FrameworkID frameworkID;

    /**
     * C'tor with List of docker images.
     *
     */
    public MesosScheduler() {
    }

    public Protos.TaskInfo addTask(String dockerImage, int maxCPU, int maxMEM, String principal, List<String> hostVolumes, List<String> containerVolumes, String... arg) {
        Protos.TaskID taskId = Protos.TaskID.newBuilder()
                .setValue(principal + "_" + Integer.toString(taskIDGenerator.incrementAndGet())).build();

        /**
         * DockerContainer Builder.
         */
        Protos.ContainerInfo.DockerInfo.Builder dockerInfoBuilder = Protos.ContainerInfo.DockerInfo.newBuilder();
        dockerInfoBuilder.setImage(dockerImage);
        dockerInfoBuilder.setNetwork(Protos.ContainerInfo.DockerInfo.Network.BRIDGE);
        /**
         * Container Builder.
         */
        Protos.ContainerInfo.Builder containerInfoBuilder = Protos.ContainerInfo.newBuilder();
        containerInfoBuilder.setType(Protos.ContainerInfo.Type.DOCKER);
        containerInfoBuilder.setDocker(dockerInfoBuilder.build());
        // Mount volumes if needed
        if (hostVolumes != null && containerVolumes != null) {
            int nHostVolumes = hostVolumes.size();
            if (nHostVolumes != containerVolumes.size()) {
                logger.error("Volume declarations of host and container differ in length ...");
            }
            
            for (int i = 0; i < nHostVolumes; i++) {
                containerInfoBuilder.addVolumes(Volume.newBuilder()
                        .setContainerPath(containerVolumes.get(i))
                        .setHostPath(hostVolumes.get(i))
                        .setMode(Volume.Mode.RW)
                        .build());
            }
        }
        /**
         * Task Builder.
         */
        Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
                .setName("task " + taskId.getValue())
                .setTaskId(taskId)
                .addResources(Protos.Resource.newBuilder()
                        .setName("cpus")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(maxCPU)))
                .addResources(Protos.Resource.newBuilder()
                        .setName("mem")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(maxMEM)))
                .setContainer(containerInfoBuilder)
                .setCommand(CommandInfo.newBuilder().setShell(false).build())
                .buildPartial(); // partialBuild, because we'll add the slaveID later

        Protos.TaskInfo taskWithArgument = Protos.TaskInfo.newBuilder(task).mergeCommand(CommandInfo.newBuilder().addAllArguments(Arrays.asList(arg)).setShell(false).build()).buildPartial();
        pendingTasks.add(taskWithArgument);
        logger.info("PendingTasks Size {}", pendingTasks.size());
        return taskWithArgument;
    }

    @Override
    public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
        logger.info("registered() master={}:{}, framework={}", masterInfo.getIp(), masterInfo.getPort(), frameworkID);
        this.frameworkID = frameworkID;
    }

    @Override
    public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
        logger.info("reregistered()");
    }

    @Override
    public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {

        logger.info("resourceOffers() with {} offers", offers.size());

        /**
         * Adding rule to devide resources. Maybe ( numberOfOffers / #containers
         * ) = amount per offer
         */
        for (final Protos.Offer offer : offers) {
            List<Protos.Resource> availableResources = offer.getResourcesList();
            double available_CPU = 0;
            double available_MEM = 0;

            /**
             * Offer Informations.
             */
            for (Protos.Resource r : availableResources) {
                switch (r.getName().toLowerCase()) {
                    case "cpus":
                        available_CPU = r.getScalar().getValue();
                        break;
                    case "mem":
                        available_MEM = r.getScalar().getValue();
                        break;
                }
            }

            List<Protos.TaskInfo> tmp = new ArrayList<>();

            while (!pendingTasks.isEmpty()) {
                double needed_CPU = 0;
                double needed_MEM = 0;
                Protos.TaskInfo actualTask = pendingTasks.get(0);

                List<Protos.Resource> actualTaskResources = actualTask.getResourcesList();
                for (Protos.Resource r : actualTaskResources) {
                    switch (r.getName().toLowerCase()) {
                        case "cpus":
                            needed_CPU = r.getScalar().getValue();
                            break;
                        case "mem":
                            needed_MEM = r.getScalar().getValue();
                            break;
                    }
                }

                if (available_CPU >= needed_CPU
                        && available_MEM >= needed_MEM) {
                    Protos.TaskInfo tmpTask = Protos.TaskInfo.newBuilder(actualTask)
                            .mergeSlaveId(offer.getSlaveId())
                            .build();
                    tmp.add(tmpTask);
                    available_CPU -= needed_CPU;
                    available_MEM -= needed_MEM;
                    pendingTasks.remove(actualTask);
                    runningTasks.put(tmpTask.getTaskId().getValue(), tmpTask);
                    logger.debug("Added Task: " + actualTask.getTaskId().getValue());
                } else {
                    logger.warn("BREAK: Task execution not possible. Offer resources exhausted");
                    break;
                }
            }
            if (!tmp.isEmpty()) {
                Protos.Filters filters = Protos.Filters.newBuilder().setRefuseSeconds(10).build();
                schedulerDriver.launchTasks(offer.getId(), tmp, filters);
                logger.info("Started Tasks: {} on Slave: ({})", tmp.size(), offer.getId().getValue());
            } else {
                schedulerDriver.declineOffer(offer.getId());
            }
        }
    }

    @Override
    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
        logger.info("offerRescinded()");
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus taskStatus) {

        final String taskId = taskStatus.getTaskId().getValue();

        logger.info("statusUpdate() task {} is in state {}",
                taskId, taskStatus.getState());

        switch (taskStatus.getState()) {
            case TASK_RUNNING:
                break;
            case TASK_FINISHED:
                finishedTasks.put(taskId, runningTasks.remove(taskId));
                break;
            default:
                /**
                 * BIIIIG PROBLEM! needs to evaluate error messages!
                 */
                logger.warn("Task-Problem: Cause = {}", taskStatus.getReason());
                switch (taskStatus.getReason()) {
                    case REASON_COMMAND_EXECUTOR_FAILED:
                        logger.info("ReScheduling Task ...");
                        pendingTasks.add(runningTasks.get(taskId));
                        runningTasks.remove(taskId);
                        break;
                }
                break;
        }

        logger.info("RUNNING Tasks={}", runningTasks.size());
        logger.info("PENDING Tasks={}", pendingTasks.size());
    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {
        logger.info("frameworkMessage()");
    }

    @Override
    public void disconnected(SchedulerDriver schedulerDriver) {
        logger.info("disconnected()");
    }

    @Override
    public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {
        logger.info("slaveLost()");
    }

    @Override
    public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {
        logger.info("executorLost()");
    }

    @Override
    public void error(SchedulerDriver schedulerDriver, String s) {
        logger.error("error() {}", s);
    }

    public List<Protos.TaskInfo> getPendingTasks() {
        return pendingTasks;
    }

    public Map<String, Protos.TaskInfo> getRunningTasks() {
        return runningTasks;
    }

    public Map<String, Protos.TaskInfo> getFinishedTasks() {
        return finishedTasks;
    }

    public Map<String, Protos.TaskInfo> getFailedTasks() {
        return failedTasks;
    }

    public FrameworkID getFramework() {
        return this.frameworkID;
    }

}