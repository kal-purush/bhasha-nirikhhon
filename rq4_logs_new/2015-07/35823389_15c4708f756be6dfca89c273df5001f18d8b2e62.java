package com.rhc.lab.drools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.drools.core.audit.event.LogEvent;
import org.drools.core.audit.event.RuleFlowNodeLogEvent;
import org.jbpm.test.JbpmJUnitBaseTestCase;
import org.jbpm.workflow.instance.WorkflowProcessInstance;
import org.junit.Before;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.manager.audit.NodeInstanceLog;
import org.kie.api.runtime.process.NodeInstance;
import org.kie.api.runtime.process.NodeInstanceContainer;
import org.kie.api.runtime.process.ProcessInstance;
import org.kie.api.runtime.process.WorkItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class constructs a ksession for the given test resources (bpmn2
 * classpath resources) for use in each BPMN test case.
 */
public class BaseBPMNTest extends JbpmJUnitBaseTestCase {

	private static final Logger logger = LoggerFactory
			.getLogger(BaseBPMNTest.class);

	private static final String HUMAN_TASK = "Human Task";
	private static final String REST_TASK = "Rest";

	/**
	 * This field can be used by all tests which extend this class to
	 * instantiate processes, complete tasks, etc.
	 * 
	 */
	protected KieSession ksession;

	/**
	 * This field is used for reference in each test case to query process
	 * variables, query tasks, etc.
	 * 
	 */
	protected ProcessInstance processInstance;

	/**
	 * This list contains all of the bpmn2 classpath resources used in an
	 * individual test case, and creates the runtime engine for each test.
	 * 
	 * e.g. ["myFirstProcess.bpmn2", "mySecondProcess.bpmn2"]
	 * 
	 */
	protected static List<String> processes = new ArrayList<String>();

	/**
	 * This map contains the process variables within each particular process
	 * 
	 */
	protected Map<String, Object> processVars = new HashMap<String, Object>();

	/**
	 * This list contains active workItems, and needs to be kept since the
	 * testWorkItemHandler deletes workItems from the JbpmJUnitBaseTestCase on
	 * retrieval.
	 */
	private List<WorkItem> workItems = new ArrayList<WorkItem>();

	/**
	 * This method is run before each individual BPMN test to set a new
	 * knowledge session with the resources required for the test to run
	 * successfully.
	 * 
	 */
	@Before
	public void createKnowledgeSession() {
		logger.info("Setting up knowledge session for resources: "
				+ processes.toString());
		createRuntimeManager(processes.toArray(new String[processes.size()]));
		ksession = getRuntimeEngine(null).getKieSession();
		ksession.getWorkItemManager().registerWorkItemHandler(HUMAN_TASK,
				getTestWorkItemHandler());
		ksession.getWorkItemManager().registerWorkItemHandler(REST_TASK,
				getTestWorkItemHandler());
		/*
		 * TODO: Register additional Work Item Handlers needed for the test
		 * suite here
		 */
		processVars.clear();
		logger.info("Knowledge session created successfully.");
	}

	/**
	 * This method sets this base class with the resource names to be injected
	 * in the ksession for each individual test.
	 * 
	 * @param resources
	 */
	public static void setProcesses(String... resources) {
		processes.clear();
		processes.addAll(Arrays.asList(resources));
	}

	/**
	 * This method loops through the ksession for outstanding work items with
	 * the given name and completes the work item with the given data outputs.
	 * 
	 * @param itemName
	 * @param itemOutput
	 * 
	 */
	public void completeWorkItem(String itemName, Map<String, Object> itemOutput) {
		boolean itemExists = false;

		workItems.addAll(getTestWorkItemHandler().getWorkItems());

		for (Iterator<WorkItem> it = workItems.iterator(); it.hasNext();) {
			WorkItem item = it.next();
			if (((String) item.getParameter("NodeName"))
					.equalsIgnoreCase(itemName)) {
				itemExists = true;
				ksession.getWorkItemManager().completeWorkItem(item.getId(),
						itemOutput);
				if (item.getState() == WorkItem.COMPLETED) {
					it.remove();
				} else {
					fail("Failed to complete the Work Item: " + itemName);
				}
				break;
			}
		}
		if (!itemExists) {
			fail("The following Work Item was not found or could not be completed: "
					+ itemName);
		}
	}

	/**
	 * This method is an opposite clone of the assertNodeActive method ootb in
	 * the JbpmJUnitBaseTestCase.
	 * 
	 * @param processInstanceId
	 * @param ksession
	 * @param name
	 */
	public void assertNodeNotActive(long processInstanceId,
			KieSession ksession, String... name) {
		List<String> names = new ArrayList<String>();
		List<String> activeNodes = new ArrayList<String>();
		for (String n : name) {
			names.add(n);
		}
		ProcessInstance processInstance = ksession
				.getProcessInstance(processInstanceId);
		if (processInstance instanceof WorkflowProcessInstance) {
			activeNodes = assertNodeNotActive(
					(WorkflowProcessInstance) processInstance, names, null);
		}
		if (!activeNodes.isEmpty()) {
			StringBuilder sb = new StringBuilder();
			int count = 0;
			for (String activeNode : activeNodes) {
				if (count != 0) {
					sb.append(", ");
				}
				sb.append(activeNode);
				count++;
			}
			fail("Node(s) active: " + sb.toString());
		}
	}

	/**
	 * This method asserts a given active task is owned by a particular username
	 * 
	 * @param container
	 * @param names
	 * @param activeNodes
	 */
	public void assertTaskOwnedBy(String taskName, String owner) {

		boolean itemExists = false;

		workItems.addAll(getTestWorkItemHandler().getWorkItems());

		for (Iterator<WorkItem> it = workItems.iterator(); it.hasNext();) {
			WorkItem item = it.next();
			if (((String) item.getParameter("NodeName"))
					.equalsIgnoreCase(taskName)) {
				if (((String) item.getParameter("ActorId")).isEmpty()
						&& (owner == null || owner == "")) {
					return;
				} else if (!((String) item.getParameter("ActorId"))
						.equalsIgnoreCase(owner)) {
					fail("Task \"" + taskName + "\" expected owner \"" + owner
							+ "\" but was \"" + item.getParameter("ActorId")
							+ "\"");
				}
			}
		}
		if (!itemExists) {
			fail("The following Work Item was not found or could not be completed: "
					+ taskName);
		}
	}

	/**
	 * This method is an opposite clone of the assertNodeActive recursive helper
	 * method ootb in the JbpmJUnitBaseTestCase.
	 * 
	 * @param processInstanceId
	 * @param ksession
	 * @param name
	 */
	public List<String> assertNodeNotActive(NodeInstanceContainer container,
			List<String> names, List<String> activeNodes) {
		if (activeNodes == null) {
			activeNodes = new ArrayList<String>();
		}
		for (NodeInstance nodeInstance : container.getNodeInstances()) {
			String nodeName = nodeInstance.getNodeName();
			if (names.contains(nodeName)) {
				activeNodes.add(nodeName);
			}
			if (nodeInstance instanceof NodeInstanceContainer) {
				return assertNodeNotActive(
						(NodeInstanceContainer) nodeInstance, names,
						activeNodes);
			}
		}
		return activeNodes;
	}

	/**
	 * This method is an opposite clone of the assertNodeTriggered ootb in the
	 * JbpmJUnitBaseTestCase.
	 * 
	 * @param processInstanceId
	 * @param nodeNames
	 */
	public void assertNodeNotTriggered(long processInstanceId,
			String... nodeNames) {
		List<String> names = new ArrayList<String>();
		List<String> triggeredNodes = new ArrayList<String>();
		for (String nodeName : nodeNames) {
			names.add(nodeName);
		}
		if (sessionPersistence) {
			List<? extends NodeInstanceLog> logs = getLogService()
					.findNodeInstances(processInstanceId);
			if (logs != null) {
				for (NodeInstanceLog l : logs) {
					String nodeName = l.getNodeName();
					if ((l.getType() == NodeInstanceLog.TYPE_ENTER || l
							.getType() == NodeInstanceLog.TYPE_EXIT)
							&& names.contains(nodeName)) {
						triggeredNodes.add(nodeName);
					}
				}
			}
		} else {
			for (LogEvent event : getInMemoryLogger().getLogEvents()) {
				if (event instanceof RuleFlowNodeLogEvent) {
					String nodeName = ((RuleFlowNodeLogEvent) event)
							.getNodeName();
					if (names.contains(nodeName)) {
						triggeredNodes.add(nodeName);
					}
				}
			}
		}
		if (!triggeredNodes.isEmpty()) {
			String s = triggeredNodes.get(0);
			for (int i = 1; i < triggeredNodes.size(); i++) {
				s += ", " + triggeredNodes.get(i);
			}
			fail("Node(s) executed: " + s);
		}
	}

}