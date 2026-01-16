package barqsoft.footballscores;

import android.annotation.TargetApi;
import android.app.PendingIntent;
import android.appwidget.AppWidgetManager;
import android.appwidget.AppWidgetProvider;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.os.Build;
import android.util.Log;
import android.widget.ListView;
import android.widget.RemoteViews;

import barqsoft.footballscores.service.FootballScoresWidgetService;

/**
 * Created by matt on 12/21/15.
 */
public class FootballScoresWidgetProvider extends AppWidgetProvider {

    private static final String TAG = FootballScoresWidgetProvider.class.getSimpleName();

    private Context context;
    private ListView scores;

    @TargetApi(Build.VERSION_CODES.HONEYCOMB)
    @Override
    public void onUpdate(Context context, AppWidgetManager appWidgetManager, int[] appWidgetIds) {
        for (int i = 0; i < appWidgetIds.length; i++) {
            int id = appWidgetIds[i];

            Intent serviceIntent = new Intent(context, FootballScoresWidgetService.class);
            serviceIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, id);
            serviceIntent.setData(Uri.parse(serviceIntent.toUri(Intent.URI_INTENT_SCHEME)));

            RemoteViews rvs = new RemoteViews(context.getPackageName(), R.layout.widget_score_list);
            rvs.setRemoteAdapter(id, R.id.widget_scores_list_view, serviceIntent);

            rvs.setEmptyView(R.id.widget_scores, R.id.widget_scores_empty);

            Intent viewIntent = new Intent(context, MainActivity.class);
            viewIntent.setAction(Intent.ACTION_VIEW);
            viewIntent.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, id);
            viewIntent.setData(Uri.parse(viewIntent.toUri(Intent.URI_INTENT_SCHEME)));

            PendingIntent viewPendingIntent = PendingIntent.getActivity(context, 0, viewIntent, 0);
            rvs.setPendingIntentTemplate(R.id.widget_scores_list_view, viewPendingIntent);

            appWidgetManager.updateAppWidget(id, rvs);
        }
        super.onUpdate(context, appWidgetManager, appWidgetIds);
    }
}