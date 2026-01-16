package cz.auderis.deploy.descriptor;

import cz.auderis.deploy.descriptor.visitor.DeploymentVisitor;
import cz.auderis.deploy.descriptor.visitor.VisitableNode;
import cz.auderis.deploy.descriptor.visitor.VisitorContext;

import java.util.ArrayList;
import java.util.List;

public class DeploymentCollection implements VisitableNode {

	final List<Deployment> deployments;

	public DeploymentCollection() {
		this.deployments = new ArrayList<Deployment>(2);
	}

	public List<Deployment> getDeployments() {
		return deployments;
	}

	@Override
	public void accept(DeploymentVisitor visitor, VisitorContext context) {
		for (final Deployment deployment : deployments) {
			deployment.accept(visitor, context);
		}
	}

}