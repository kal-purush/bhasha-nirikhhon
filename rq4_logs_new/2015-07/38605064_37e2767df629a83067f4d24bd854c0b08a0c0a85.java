package treeptik.ldallen;

import com.github.aesteve.vertx.nubes.VertxNubes;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.jdbc.JDBCAuth;
import io.vertx.ext.auth.shiro.ShiroAuth;
import io.vertx.ext.auth.shiro.ShiroAuthRealmType;
import io.vertx.ext.jdbc.JDBCClient;
import treeptik.ldallen.services.TaskService;

import static com.github.aesteve.vertx.nubes.utils.async.AsyncUtils.*;

public class Server extends AbstractVerticle {


	private HttpServer server;
	private HttpServerOptions options;
	private VertxNubes nubes;

	@Override
	public void init(Vertx vertx, Context context) {
		super.init(vertx, context);
		JsonObject config = context.config();
		options = new HttpServerOptions();
		options.setHost(config.getString("host", "localhost"));
		options.setPort(config.getInteger("port", 9000));
		JsonObject jdbcConfig = config.getJsonObject("jdbcConf");
		nubes = new VertxNubes(vertx, config);
		JDBCClient client = JDBCClient.createShared(vertx, jdbcConfig,"todolist");
		JsonObject shiroConf =  new JsonObject().put("properties_path", "web/assets/vertx-users.properties");
		AuthProvider shiroAuth = ShiroAuth.create(vertx, ShiroAuthRealmType.PROPERTIES,shiroConf);

		nubes.setAuthProvider(shiroAuth);
		nubes.registerService("taskService", new TaskService(jdbcConfig));

	}

	@Override
	public void start(Future<Void> future) {
		server = vertx.createHttpServer(options);
		nubes.bootstrap(onSuccessOnly(future, router -> {
			server.requestHandler(router::accept);
			server.listen(ignoreResult(future));
		}));
	}

	@Override
	public void stop(Future<Void> future) {
		nubes.stop(nubesRes -> {
			closeServer(future);
		});
	}

	private void closeServer(Future<Void> future) {
		if (server != null) {
			server.close(completeOrFail(future));
		} else {
			future.complete();
		}
	}

}