package dev.stocky37.xiv.actions.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import dev.stocky37.xiv.actions.core.RotationService;
import dev.stocky37.xiv.actions.data.Rotation;
import dev.stocky37.xiv.actions.data.RotationInput;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/rotation")
@Produces(APPLICATION_JSON)
public class RotationApi {

	private final RotationService rotations;

	@Inject
	public RotationApi(RotationService rotations) {
		this.rotations = rotations;
	}

	@POST
	@Consumes(APPLICATION_JSON)
	public Rotation test(RotationInput input) {
		return rotations.buildRotation(input);
	}

}
