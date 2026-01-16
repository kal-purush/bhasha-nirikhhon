package com.ferit.davor.smarthouse;

import com.ferit.davor.smarthouse.Connection.ConnectionStatus;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;

import android.content.Context;
import android.widget.Toast;

/**
 * This Class handles receiving information from the
 * MqttAndroidClient and updating the Connection associated with
 * the action
 */
class ActionListener implements IMqttActionListener {

  enum Action {
    /** Connect Action **/
    CONNECT,
    /** Disconnect Action **/
    DISCONNECT,
    /** Subscribe Action **/
    SUBSCRIBE,
    /** Publish Action **/
    PUBLISH
  }


  private Action action;
  /** The arguments passed to be used for formatting strings**/
  private String[] additionalArgs;
  /** Handle of the Connection this action was being executed on **/
  private String clientHandle;

  private Context context;

  public ActionListener(Context context, Action action,
      String clientHandle, String... additionalArgs) {
    this.context = context;
    this.action = action;
    this.clientHandle = clientHandle;
    this.additionalArgs = additionalArgs;
  }

  @Override
  public void onSuccess(IMqttToken asyncActionToken) {
    switch (action) {
      case CONNECT :
        connect();
        break;
      case DISCONNECT :
        disconnect();
        break;
      case SUBSCRIBE :
        subscribe();
        break;
      case PUBLISH :
        publish();
        break;
    }

  }

  private void publish() {

    Connection c = Connections.getInstance(context).getConnection(clientHandle);
    String actionTaken = context.getString(R.string.toast_pub_success,
        (Object[]) additionalArgs);
    c.addAction(actionTaken);
    Notify.toast(context, actionTaken, Toast.LENGTH_SHORT);
  }

  private void subscribe() {
    Connection c = Connections.getInstance(context).getConnection(clientHandle);
    String actionTaken = context.getString(R.string.toast_sub_success,
        (Object[]) additionalArgs);
    c.addAction(actionTaken);
    Notify.toast(context, actionTaken, Toast.LENGTH_SHORT);

  }

  private void disconnect() {
    Connection c = Connections.getInstance(context).getConnection(clientHandle);
    c.changeConnectionStatus(ConnectionStatus.DISCONNECTED);
    String actionTaken = context.getString(R.string.toast_disconnected);
    c.addAction(actionTaken);

  }

  private void connect() {

    Connection c = Connections.getInstance(context).getConnection(clientHandle);
    c.changeConnectionStatus(Connection.ConnectionStatus.CONNECTED);
    c.addAction("Client Connected");

  }

  @Override
  public void onFailure(IMqttToken token, Throwable exception) {
    switch (action) {
      case CONNECT :
        connect(exception);
        break;
      case DISCONNECT :
        disconnect(exception);
        break;
      case SUBSCRIBE :
        subscribe(exception);
        break;
      case PUBLISH :
        publish(exception);
        break;
    }

  }

  private void publish(Throwable exception) {
    Connection c = Connections.getInstance(context).getConnection(clientHandle);
    String action = context.getString(R.string.toast_pub_failed,
        (Object[]) additionalArgs);
    c.addAction(action);
    Notify.toast(context, action, Toast.LENGTH_SHORT);

  }

  private void subscribe(Throwable exception) {
    Connection c = Connections.getInstance(context).getConnection(clientHandle);
    String action = context.getString(R.string.toast_sub_failed,
        (Object[]) additionalArgs);
    c.addAction(action);
    Notify.toast(context, action, Toast.LENGTH_SHORT);

  }

  private void disconnect(Throwable exception) {
    Connection c = Connections.getInstance(context).getConnection(clientHandle);
    c.changeConnectionStatus(ConnectionStatus.DISCONNECTED);
    c.addAction("Disconnect Failed - an error occured");

  }

  private void connect(Throwable exception) {
    Connection c = Connections.getInstance(context).getConnection(clientHandle);
    c.changeConnectionStatus(Connection.ConnectionStatus.ERROR);
    c.addAction("Client failed to connect");

  }

}