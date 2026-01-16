package kubernetes.client.mapper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.springframework.stereotype.Repository;

import kubernetes.client.model.Application;
import kubernetes.client.model.Resources;

@Repository
public class ResourcesMapper {

	InfluxDB influxDB = InfluxDBFactory.connect("http://192.168.5.10:30086", "root", "root");

	public Resources get(Application app) {

		long cpu = 0;
		float usage_rate = 0;
		float request = 0;

		Resources resources = new Resources();
		List<List<Long>> actualCPU = new ArrayList<List<Long>>();
		List<List<Long>> predictCPU = new ArrayList<List<Long>>();

		String stringNow = "";
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

		Map<String, String> podLabels = app.getListPod().get(0).getMetadata().getLabels();

		String labels = "app:" + podLabels.get("app") + ",pod-template-hash:" + podLabels.get("pod-template-hash");
		QueryResult queryResult1 = influxDB.query(new Query(
				"SELECT sum(\"value\") FROM \"cpu/usage_rate\" WHERE \"type\" = 'pod' AND \"namespace_name\" =~ /"
						+ app.getDeployment().getMetadata().getNamespace() + "/ AND \"labels\" =~ /" + labels
						+ "/ AND time >= now() - 10m GROUP BY time(1m)",
				"k8s"));

		QueryResult queryResult2 = influxDB.query(new Query(
				"SELECT sum(\"value\") FROM \"cpu/request\" WHERE \"type\" = 'pod' AND \"namespace_name\" =~ /"
						+ app.getDeployment().getMetadata().getNamespace() + "/ AND \"labels\" =~ /" + labels
						+ "/ AND time >= now() - 10m GROUP BY time(1m)",
				"k8s"));
		QueryResult queryResult3 = influxDB.query(new Query("SELECT value FROM \"cpu/predict\" WHERE \"id\" =~ /"
				+ app.getId() + "/ AND time > now() - 13m AND time < now() - 1m", "k8s"));
		long now = System.currentTimeMillis();
		now = now - now % 60000;
		if (queryResult1.getResults().get(0).getSeries() == null
				|| queryResult2.getResults().get(0).getSeries() == null) {
			long startTime = now - 9 * 60000;
			for (int i = 0; i < 10; i++) {
				List<Long> data = new ArrayList<Long>();
				data.add(startTime);
				data.add(0l);
				actualCPU.add(data);
				startTime += 60000;
			}
		} else {
			List<List<Object>> values1 = queryResult1.getResults().get(0).getSeries().get(0).getValues();
			List<List<Object>> values2 = queryResult2.getResults().get(0).getSeries().get(0).getValues();
			stringNow = values1.get(values1.size() - 1).get(0).toString();

			for (int i = 1; i < values1.size(); i++) {
				List<Long> data = new ArrayList<Long>();
				String dateString = values1.get(i).get(0).toString();
				long dateLong = 0l;
				try {
					Date date = formatter.parse(dateString.replaceAll("Z$", "+0000"));
					dateLong = date.getTime();
				} catch (ParseException e) {
					e.printStackTrace();
				}
				data.add(dateLong);
				if (values1.get(i).get(1) != null) {
					usage_rate = Float.valueOf(values1.get(i).get(1).toString());
				}
				if (values2.get(i).get(1) != null) {
					request = Float.valueOf(values2.get(i).get(1).toString());
				}
				if (request != 0) {
					cpu = (long) ((usage_rate / request) * 100);
				}
				data.add(cpu);
				actualCPU.add(data);
			}
		}

		if (queryResult3.getResults().get(0).getSeries() == null) {

			long startTime = now - 9 * 60000;
			for (int i = 0; i < 12; i++) {
				List<Long> data = new ArrayList<Long>();
				data.add(startTime);
				data.add(0l);
				predictCPU.add(data);
				startTime += 60000;
			}
		} else {
			List<List<Object>> values = queryResult3.getResults().get(0).getSeries().get(0).getValues();
			System.out.println(values);
			if (!stringNow.isEmpty()) {
				try {
					Date date = formatter.parse(stringNow.replaceAll("Z$", "+0000"));
					now = date.getTime();
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			long startTime = now - 12 * 60000;

			for (int i = 0; i < 12; i++) {
				long valueCPU = 0l;
				List<Long> data = new ArrayList<Long>();
				for (int j = 0; j < values.size(); j++) {
					String dateString = values.get(j).get(0).toString();
					long dateLong = 0l;
					try {
						Date date = formatter.parse(dateString.replaceAll("Z$", "+0000"));
						dateLong = date.getTime();
						if (startTime == dateLong) {
							float valueFloat = Float.valueOf(values.get(j).get(1).toString());
							valueCPU = (long) valueFloat;
							break;
						}
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}

				data.add(startTime + 60000 * 3);
				data.add(valueCPU);
				predictCPU.add(data);
				startTime += 60000;
			}
		}

		resources.setActualCPU(actualCPU);
		resources.setPredictCPU(predictCPU);
		return resources;
	}
}