package eu.javeo.knowler.client.mobile.knowler;

import android.app.Fragment;
import android.content.Context;
import android.os.Bundle;
import android.support.v4.view.PagerAdapter;
import android.support.v4.view.ViewPager;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import butterknife.ButterKnife;
import butterknife.InjectView;
import de.greenrobot.event.EventBus;
import eu.javeo.knowler.client.mobile.knowler.event.ApplicationEvent;
import eu.javeo.knowler.client.mobile.knowler.event.ChangedMovieTimeEvent;
import eu.javeo.knowler.client.mobile.knowler.event.EventBusListener;
import eu.javeo.knowler.client.mobile.knowler.event.PageSelectedEvent;
import eu.javeo.knowler.client.mobile.knowler.event.youtube.SeekToEvent;
import java.util.ArrayList;
import java.util.Arrays;

public class SlidesFragment extends Fragment implements EventBusListener<ApplicationEvent> {

	@InjectView(R.id.pager_container)
	protected SlidesImagesContainer slidesImagesContainer;

	private String profileImages[] =
			{"profile_2", "profile_3", "profile_4", "profile_5", "profile_6", "profile_7", "profile_8", "profile_9", "profile_10", "profile_11",
					"profile_12", "profile_13", "profile_14", "profile_15", "profile_16", "profile_17", "profile_18", "profile_19", "profile_20", "profile_21"};

	private ViewPager pager;

	public static SlidesFragment newInstance() {
		SlidesFragment fragment = new SlidesFragment();
		return fragment;
	}

	@Override
	public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
		View inflate = inflater.inflate(R.layout.fragment_slides, container, false);
		ButterKnife.inject(this, inflate);
		EventBus.getDefault().register(this);
		ArrayList<String> images = new ArrayList<String>(Arrays.asList(profileImages));

		pager = slidesImagesContainer.getViewPager();
		PagerAdapter adapter = new ImagePagerAdapter();
		pager.setAdapter(adapter);
		pager.setOffscreenPageLimit(adapter.getCount());
		pager.setClipChildren(true);
		pager.setCurrentItem(1);

		return inflate;
	}

	@Override
	public void onDestroy() {
		super.onDestroy();
		EventBus.getDefault().unregister(this);
	}

	@Override
	public void onEvent(ApplicationEvent event) {
		if (event instanceof PageSelectedEvent) {
			// Bo tutaj będę miał informacje o filmach
			pager.getCurrentItem();
			EventBus.getDefault().post(new ChangedMovieTimeEvent());
		} else if (event instanceof SeekToEvent) {
			int millis = ((SeekToEvent) event).getMillis();
		}
	}

	public class ImagePagerAdapter extends PagerAdapter {

		@Override
		public int getCount() {
			return profileImages.length;
		}

		@Override
		public boolean isViewFromObject(View view, Object object) {
			return view == object;
		}

		@Override
		public Object instantiateItem(ViewGroup container, int position) {
			Context context = getActivity();
			ImageView imageView = new ImageView(context);
			imageView.setScaleType(ImageView.ScaleType.CENTER_CROP);
			int drawable = context.getResources().getIdentifier(profileImages[position], "drawable", context.getPackageName());
			imageView.setImageResource(drawable);
			imageView.setAdjustViewBounds(true);
			container.addView(imageView, 0);
			return imageView;
		}

		@Override
		public void destroyItem(ViewGroup container, int position, Object object) {
			container.removeView((ImageView) object);
		}
	}
}