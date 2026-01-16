package treeptik.ldallen.controller.jwt;

import com.github.aesteve.vertx.nubes.annotations.Controller;
import com.github.aesteve.vertx.nubes.annotations.File;
import com.github.aesteve.vertx.nubes.annotations.auth.Auth;
import com.github.aesteve.vertx.nubes.annotations.routing.http.GET;
import com.github.aesteve.vertx.nubes.auth.AuthMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTOptions;
import io.vertx.ext.web.RoutingContext;
import treeptik.ldallen.Server;

import java.util.ArrayList;
import java.util.List;

@Controller("/")
public class JWTController {

	@GET
	@File
	public String rootView() {
		return "web/assets/index.html";
	}

	@GET("token")
	public void token(RoutingContext context){
		context.response().putHeader("Content-Type", "text/plain");
		List<String> authority = new ArrayList<String>();
		authority.add("secret");
		String token = Server.jwt.generateToken(new JsonObject(), new JWTOptions().setExpiresInSeconds(60).setPermissions(authority));
		context.response().end(token);
	}

	@GET("private")
	@Auth(method = AuthMethod.JWT, authority = "secret")
	@File
	public String listView() {
		return "web/assets/private.html";
	}


}