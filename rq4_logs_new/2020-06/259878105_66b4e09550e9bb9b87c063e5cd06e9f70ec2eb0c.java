package org.folio.rest.client;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.folio.rest.jaxrs.model.Item;
import org.folio.rest.jaxrs.model.Location;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;

public class InventoryStorageClient extends OkapiClient {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public InventoryStorageClient(Vertx vertx, Map<String, String> okapiHeaders) {
    super(WebClient.create(vertx), okapiHeaders);
  }

  public Future<Item> findItemById(String itemId) {
    return fetchById("item-storage/items", itemId, Item.class);
  }

  public Future<Location> findLocationById(String locationId) {
    return fetchById("locations", locationId, Location.class);
  }
}