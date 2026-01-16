package com.dam.asfaltame;
import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import com.dam.asfaltame.Database.AppRepository;
import com.dam.asfaltame.Modelo.Report;
import com.dam.asfaltame.Modelo.Status;
import java.util.List;

public class ReportAdapter extends ArrayAdapter<Report> {

    private Context ctx;
    private List<Report> data;
    private AppRepository appRepository;

    public ReportAdapter(Context context, List<Report> objects) {
        super(context, 0, objects);
        this.ctx = context;
        this.data = objects;
        appRepository = AppRepository.getInstance(context);
    }

    public View getView(int position, View convertView, ViewGroup parent) {
        View reportListLine = convertView;
        if (reportListLine == null) {
            reportListLine = LayoutInflater.from(this.ctx).inflate(R.layout.report_list_line, parent, false);
        }

        ReportHolder holder = (ReportHolder) reportListLine.getTag();
        if (holder == null) {
            holder = new ReportHolder(reportListLine);
            reportListLine.setTag(holder);
        }

        Report report = this.data.get(position);

        holder.reportAddress.setText(report.getAddress());

        if(report.getStatus() == Status.EN_REPARACION){
            holder.reportIcon.setImageResource(R.drawable.ic_reparacion);
        }else{
            switch (report.getReportType()){
                case BACHE:
                    holder.reportIcon.setImageResource(R.drawable.ic_bache);
                    break;
                case HUNDIMIENTO:
                    holder.reportIcon.setImageResource(R.drawable.ic_hundimiento);
                    break;
                case TAPA_HUNDIDA:
                    holder.reportIcon.setImageResource(R.drawable.ic_tapa_hundida);
                    break;
                case MULTIPLE:
                    holder.reportIcon.setImageResource(R.drawable.ic_multiple);
                    break;
            }
        }

        return reportListLine;
    }
}