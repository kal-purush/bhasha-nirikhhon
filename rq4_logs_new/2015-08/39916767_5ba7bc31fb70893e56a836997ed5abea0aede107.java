package com.layer.atlas.cells;

import android.content.Context;
import android.content.Intent;
import android.graphics.drawable.GradientDrawable;
import android.net.Uri;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import com.layer.atlas.Atlas;
import com.layer.atlas.AtlasMessagesList;
import com.layer.atlas.AtlasProgressView;
import com.layer.atlas.R;
import com.layer.atlas.ShapedFrameLayout;
import com.layer.sdk.listeners.LayerProgressListener;
import com.layer.sdk.messaging.MessagePart;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by garhiggins on 7/31/15.
 */
public class CamblyAttachmentCell extends AtlasMessagesList.Cell implements View.OnClickListener, LayerProgressListener {

  protected MessagePart attachment;
  protected String filename;
  AtlasMessagesList messagesList;

  /** if more than 0 - download is in progress */
  volatile long downloadProgressBytes = -1;

  public CamblyAttachmentCell(String filename, MessagePart attachment, AtlasMessagesList messagesList) {
    super(attachment);
    this.attachment = attachment;
    this.filename = filename;
    this.messagesList = messagesList;
  }

  public View onBind(ViewGroup cellContainer) {

    ViewGroup rootView = (ViewGroup) Atlas.Tools.findChildById(cellContainer, R.id.atlas_view_messages_cell_attachment);
    if (rootView == null) {
      rootView = (ViewGroup) LayoutInflater.from(cellContainer.getContext()).inflate(R.layout.atlas_view_messages_cell_attachment, cellContainer, false);
    }

    boolean myMessage = messagesList.getLayerClient().getAuthenticatedUserId().equals(messagePart.getMessage().getSender().getUserId());

    View attachmentContainerMy = rootView.findViewById(R.id.atlas_view_messages_cell_attachment_container_my);
    ((GradientDrawable)attachmentContainerMy.getBackground()).setColor(messagesList.myBubbleColor);
    TextView textViewMy = (TextView) attachmentContainerMy.findViewById(R.id.atlas_view_messages_convert_text_my);
    textViewMy.setTextColor(messagesList.myTextColor);
    attachmentContainerMy.setVisibility(myMessage ? View.VISIBLE : View.GONE);
    attachmentContainerMy.setOnClickListener(this);

    View attachmentContainerTheir = rootView.findViewById(R.id.atlas_view_messages_cell_attachment_container_their);
    ((GradientDrawable)attachmentContainerTheir.getBackground()).setColor(messagesList.otherBubbleColor);
    TextView textViewTheir = (TextView) attachmentContainerTheir.findViewById(R.id.atlas_view_messages_convert_text_their);
    textViewTheir.setTextColor(messagesList.otherTextColor);
    attachmentContainerTheir.setVisibility(!myMessage ? View.VISIBLE : View.GONE);
    attachmentContainerTheir.setOnClickListener(this);

    TextView textView = myMessage ? textViewMy : textViewTheir;
    textView.setText(filename);


    AtlasProgressView progressMy = (AtlasProgressView) rootView.findViewById(R.id.atlas_view_messages_cell_attachment_my_progress);
    AtlasProgressView progressTheir = (AtlasProgressView) rootView.findViewById(R.id.atlas_view_messages_cell_attachment_their_progress);
    AtlasProgressView progressView = myMessage ? progressMy : progressTheir;

    if (downloadProgressBytes > 0) {
      float progress = 1.0f * downloadProgressBytes / attachment.getSize();
      progressView.setVisibility(View.VISIBLE);
      progressView.setProgress(progress);
    } else {
      progressView.setVisibility(View.GONE);
    }

    ShapedFrameLayout cellCustom = (ShapedFrameLayout) (myMessage ? attachmentContainerMy : attachmentContainerTheir);
    // clustering
    cellCustom.setCornerRadiusDp(16, 16, 16, 16);
    if (!AtlasMessagesList.CLUSTERED_BUBBLES) return rootView;
    if (myMessage) {
      if (this.clusterHeadItemId == this.clusterItemId && !this.clusterTail) {
        cellCustom.setCornerRadiusDp(16, 16, 2, 16);
      } else if (this.clusterTail && this.clusterHeadItemId != this.clusterItemId) {
        cellCustom.setCornerRadiusDp(16, 2, 16, 16);
      } else if (this.clusterHeadItemId != this.clusterItemId && !this.clusterTail) {
        cellCustom.setCornerRadiusDp(16, 2, 2, 16);
      }
    } else {
      if (this.clusterHeadItemId == this.clusterItemId && !this.clusterTail) {
        cellCustom.setCornerRadiusDp(16, 16, 16, 2);
      } else if (this.clusterTail && this.clusterHeadItemId != this.clusterItemId) {
        cellCustom.setCornerRadiusDp(2, 16, 16, 16);
      } else if (this.clusterHeadItemId != this.clusterItemId && !this.clusterTail) {
        cellCustom.setCornerRadiusDp(2, 16, 16, 2);
      }
    }
    return rootView;
  }

  @Override
  public void onClick(View v) {
    if (!attachment.isContentReady() && downloadProgressBytes == -1) {
      attachment.download(this);
    } else {
      openAttachment(v.getContext());
    }
  }

  // LayerDownloadListener (when downloading part)
  @Override
  public void onProgressStart(MessagePart part, LayerProgressListener.Operation operation) {
  }
  @Override
  public void onProgressUpdate(MessagePart part, LayerProgressListener.Operation operation, long transferredBytes) {
    downloadProgressBytes = transferredBytes;
    messagesList.requestRefresh();
  }
  @Override
  public void onProgressError(MessagePart part, LayerProgressListener.Operation operation, Throwable cause) {
    downloadProgressBytes = -1;
    messagesList.requestRefresh();
  }
  @Override
  public void onProgressComplete(MessagePart part, LayerProgressListener.Operation operation) {
    downloadProgressBytes = -1;
    messagesList.requestRefresh();
    // TODO(gar): open the attachment? I need a context, but Layer didn't give me one :(
  }

  void openAttachment(Context context) {
    try {
      //Attachment is a structured data object that contains of a byte[], filename and mimetype (like image/jpeg)
      FileOutputStream fos = context.openFileOutput(filename, Context.MODE_WORLD_READABLE);
      fos.write(attachment.getData());
      fos.flush();
      fos.close();
      File f = context.getFileStreamPath(filename);
      Uri uri = Uri.fromFile(f);
      Intent intent = new Intent(Intent.ACTION_VIEW);
      intent.setDataAndType(uri, attachment.getMimeType());
      context.startActivity(intent);
    } catch (IOException e) {
      e.printStackTrace(); // LOG it too...
    }
  }
}