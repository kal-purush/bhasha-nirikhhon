package eu.javeo.knowler.client.mobile.knowler;

import android.app.Fragment;
import android.content.Context;
import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;
import butterknife.ButterKnife;
import butterknife.InjectView;
import com.google.android.youtube.player.YouTubeInitializationResult;
import com.google.android.youtube.player.YouTubeThumbnailLoader;
import com.google.android.youtube.player.YouTubeThumbnailView;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelectPresentationAdapter extends RecyclerView.Adapter<SelectPresentationAdapter.ViewHolder> {

	@SuppressWarnings("unused")
	private static final String TAG = SelectPresentationAdapter.class.getSimpleName();

	private final List<VideoEntry> entries;

	private Context context;

	private final Map<YouTubeThumbnailView, YouTubeThumbnailLoader> thumbnailViewToLoaderMap;

	private static final List<VideoEntry> VIDEO_LIST;

	static {
		List<VideoEntry> list = new ArrayList<VideoEntry>();
		list.add(new VideoEntry("YouTube Collection", "Y_UmWdcTrrc"));
		list.add(new VideoEntry("GMail Tap", "1KhZKNZO8mQ"));
		list.add(new VideoEntry("Chrome Multitask", "UiLSiqyDf4Y"));
		list.add(new VideoEntry("Google Fiber", "re0VRK6ouwI"));
		list.add(new VideoEntry("Autocompleter", "blB_X38YSxQ"));
		list.add(new VideoEntry("GMail Motion", "Bu927_ul_X0"));
		list.add(new VideoEntry("Translate for Animals", "3I24bSteJpw"));
		list.add(new VideoEntry("Pink Floyd", "BJwLiWpXYVI"));
		VIDEO_LIST = Collections.unmodifiableList(list);
	}

	private static final class VideoEntry {

		private final String text;

		private final String videoId;

		public VideoEntry(String text, String videoId) {
			this.text = text;
			this.videoId = videoId;
		}
	}

	public SelectPresentationAdapter(Context context) {
		this.entries = VIDEO_LIST;
		this.context = context;
		this.thumbnailViewToLoaderMap = new HashMap<YouTubeThumbnailView, YouTubeThumbnailLoader>();
		this.thumbnailListener = new ThumbnailListener();
	}

	@Override
	public ViewHolder onCreateViewHolder(ViewGroup viewGroup, int viewType) {
		View v = LayoutInflater.from(viewGroup.getContext()).inflate(R.layout.fragment_row_presentation, viewGroup, false);
		return new ViewHolder(v);
	}

	@Override
	public void onBindViewHolder(ViewHolder vh, int index) {

		VideoEntry entry = entries.get(index);

		vh.textViewName.setText(entry.text);
		vh.thumbnail.setTag(entry.videoId);

		YouTubeThumbnailLoader loader = thumbnailViewToLoaderMap.get(vh.thumbnail);

		if (loader == null) {
			vh.thumbnail.setTag(entry.videoId);
		} else {
			vh.thumbnail.setImageResource(R.drawable.loading_thumbnail);
			loader.setVideo(entry.videoId);
		}
	}

	@Override
	public int getItemCount() {
		return entries.size();
	}

	protected void showFragment(Fragment fragment, String title) {
		context.sendBroadcast(new Intent(MainActivity.ACTION_FRAGMENT_CHANGED).putExtra(MainActivity.EXTRA_FRAGMENT_TITLE, title));
		((AppCompatActivity) context).getFragmentManager().beginTransaction().replace(R.id.content, fragment).addToBackStack(null).commit();
	}

	public class ViewHolder extends RecyclerView.ViewHolder implements View.OnClickListener {

		@InjectView(R.id.textViewName)
		protected TextView textViewName;

		@InjectView(R.id.thumbnail)
		protected YouTubeThumbnailView thumbnail;

		public ViewHolder(View v) {
			super(v);
			ButterKnife.inject(this, v);
			v.setOnClickListener(this);
			thumbnail.initialize(DeveloperKey.DEVELOPER_KEY, thumbnailListener);
		}

		@Override
		public void onClick(View view) {
			Intent intent = new Intent();
			intent.setClass(context, ShowPresentationActivity.class);
			intent.putExtra(ShowPresentationActivity.EXTRA_VIDEO_ID, entries.get(getAdapterPosition()).videoId);
			context.startActivity(intent);
		}
	}

	private final ThumbnailListener thumbnailListener;

	private final class ThumbnailListener implements
			YouTubeThumbnailView.OnInitializedListener,
			YouTubeThumbnailLoader.OnThumbnailLoadedListener {

		@Override
		public void onInitializationSuccess(
				YouTubeThumbnailView view, YouTubeThumbnailLoader loader) {
			loader.setOnThumbnailLoadedListener(this);
			thumbnailViewToLoaderMap.put(view, loader);
			view.setImageResource(R.drawable.loading_thumbnail);
			String videoId = (String) view.getTag();
			loader.setVideo(videoId);
		}

		@Override
		public void onInitializationFailure(YouTubeThumbnailView view, YouTubeInitializationResult loader) {
			view.setImageResource(R.drawable.no_thumbnail);
		}

		@Override
		public void onThumbnailLoaded(YouTubeThumbnailView view, String videoId) {
		}

		@Override
		public void onThumbnailError(YouTubeThumbnailView view, YouTubeThumbnailLoader.ErrorReason errorReason) {
			view.setImageResource(R.drawable.no_thumbnail);
		}
	}

	public void releaseLoaders() {
		for (YouTubeThumbnailLoader loader : thumbnailViewToLoaderMap.values()) {
			loader.release();
		}
	}
}