package gradebook;

import java.net.URI;
import java.util.HashMap;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.json.JSONObject;

@Path("/GradeBook")
public class GradebookResource {

	public static HashMap<String, HashMap<String, Student>> grades = new HashMap<String, HashMap<String, Student>>();
	@Context
	private UriInfo context;

	@GET
	@Path("/gradeitem/{gradeitemid}/studentid/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response read(@PathParam("gradeitemid") String gradeitemid, @PathParam("id") String id) {
		Response response;
		String studentid, gradeitem, marks, feedback;
		gradeitem = gradeitemid;
		studentid = id;
		try {
			if (grades.containsKey(gradeitem)) {

				if (grades.get(gradeitem).containsKey(studentid)) {

					Student s = grades.get(gradeitem).get(studentid);
					marks = s.getMarks();
					feedback = s.getFeedback();
					JSONObject obj = new JSONObject();
					obj.put("student_id", studentid);
					obj.put("grade_item", gradeitem);
					obj.put("marks", marks);
					obj.put("feedback", feedback);
					URI locationURI = URI
							.create(context.getAbsolutePath() + "/gradeitem/" + gradeitem + "/studentid/" + studentid);
					response = Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON)
							.location(locationURI).entity(obj.toString()).build();

					return response;
				}

				else {
					response = Response.status(Response.Status.NOT_FOUND).entity("Student does not exist").build();
					return response;
				}

			} else {
				response = Response.status(Response.Status.NOT_FOUND).entity("GradeItem does not exist").build();
				return response;
			}
		} catch (Exception e) {
			System.out.println(e);
			response = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("ERROR").build();
			return response;

		}

	}

	@Path("/creategradeitem")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response creategradeitem(String item) {

		JSONObject myobject = new JSONObject(item);
		String name = new String();
		name = myobject.getString("grade_item");
		try {
			if (grades.containsKey(name)) {
				Response res = Response.status(Response.Status.CONFLICT).type(MediaType.APPLICATION_JSON)
						.entity("Already Exists").build();
				return res;
			} else {
				HashMap<String, Student> temp = new HashMap<String, Student>();
				grades.put(name, temp);

				Response res = Response.status(Response.Status.CREATED).type(MediaType.APPLICATION_JSON).entity("ADDED")
						.build();
				return res;

			}
		} catch (Exception e) {
			Response res = Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON)
					.entity("ERROR").build();
			return res;

		}

	}

	@Path("/create")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response create(String item) {

		JSONObject myobject = new JSONObject(item);
		String studentid, gradeitem, marks, feedback;
		studentid = myobject.getString("student_id");
		gradeitem = myobject.getString("grade_item");
		marks = myobject.getString("marks");
		feedback = myobject.getString("feedback");

		if (grades.containsKey(gradeitem)) {
			try {

				HashMap<String, Student> temp = new HashMap<String, Student>(grades.get(gradeitem));
				if (!temp.containsKey(studentid)) {

					if (!marks.isEmpty()) {
						Student s = new Student(studentid, marks, feedback);
						temp.put(studentid, s);
						grades.put(gradeitem, temp);
						URI locationURI = URI.create(
								context.getAbsolutePath() + "/gradeitem/" + gradeitem + "/studentid/" + studentid);
						Response res = Response.status(Response.Status.CREATED).type(MediaType.APPLICATION_JSON)
								.location(locationURI).entity("Added").build();

						return res;
					} else {
						URI locationURI = URI.create(
								context.getAbsolutePath() + "/gradeitem/" + gradeitem + "/studentid/" + studentid);
						Response res = Response.status(Response.Status.NOT_ACCEPTABLE).type(MediaType.APPLICATION_JSON)
								.location(locationURI).entity("Enter grade").build();
						return res;
					}
				} else {
					URI locationURI = URI
							.create(context.getAbsolutePath() + "/gradeitem/" + gradeitem + "/studentid/" + studentid);
					Response res = Response.status(Response.Status.CONFLICT).type(MediaType.APPLICATION_JSON)
							.location(locationURI).entity("Already entry exists").build();
					return res;
				}

			} catch (Exception e) {
				Response res = Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON)
						.entity("ERROR").build();
				System.out.println(e);
				return res;

			}
		} else {
			Response res = Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON)
					.entity("Grade Item does not exist").build();
			return res;

		}

	}

	@PUT
	@Path("/gradeitem/{gradeitemid}/studentid/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response update(@PathParam("gradeitemid") String gradeitemid, @PathParam("id") String id, String item) {

		JSONObject myobject = new JSONObject(item);
		String studentid, gradeitem, marks, feedback;
		studentid = myobject.getString("student_id");
		gradeitem = myobject.getString("grade_item");
		marks = myobject.getString("marks");
		feedback = myobject.getString("feedback");

		if (grades.containsKey(gradeitem)) {
			try {
				System.out.println(grades.keySet());

				Student s = new Student(studentid, marks, feedback);
				HashMap<String, Student> temp = new HashMap<String, Student>(grades.get(gradeitem));
				if (temp.containsKey(studentid)) {
					temp.put(studentid, s);
					grades.put(gradeitem, temp);
					System.out.println(temp.get(studentid).getMarks());
					URI locationURI = URI
							.create(context.getAbsolutePath() + "/gradeitem/" + gradeitem + "/studentid/" + studentid);
					Response res = Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON)
							.location(locationURI).entity("Added").build();

					return res;
				} else {
					Response res = Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON)
							.entity("Student does not exist").build();

					return res;
				}

			} catch (Exception e) {
				Response res = Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON)
						.entity("ERROR").build();
				System.out.println(e);
				return res;

			}
		} else {
			Response res = Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON)
					.entity("Grade Item does not exist").build();
			return res;

		}

	}

	@DELETE
	@Path("/gradeitem/{gradeitemid}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteGradeItem(@PathParam("gradeitemid") String id) {
		if (grades.containsKey(id)) {

			grades.remove(id);
			System.out.println(grades.keySet());
			URI locationURI = URI.create(context.getAbsolutePath() + "/gradeitem/" + id);
			Response res = Response.status(Response.Status.NO_CONTENT).type(MediaType.APPLICATION_JSON)
					.location(locationURI).entity("Deleted").build();
			return res;
		}

		else {
			Response res = Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON)
					.entity("Not found").build();
			return res;
		}

	}

	@DELETE
	@Path("/gradeitem/{gradeitemid}/studentid/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteGrade(@PathParam("gradeitemid") String gradeitemid, @PathParam("id") String id) {
		if (grades.containsKey(gradeitemid)) {
			if (grades.get(gradeitemid).containsKey(id)) {
				grades.get(gradeitemid).remove(id);
				URI locationURI = URI
						.create(context.getAbsolutePath() + "/gradeitem/" + gradeitemid + "/studentid/" + id);
				Response res = Response.status(Response.Status.NO_CONTENT).type(MediaType.APPLICATION_JSON)
						.location(locationURI).entity("Deleted").build();
				return res;
			} else {
				Response res = Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON)
						.entity("Not found studentid").build();
				return res;
			}
		}

		else {
			Response res = Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON)
					.entity("Not found gradeitem").build();
			return res;
		}

	}
}