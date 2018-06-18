import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;

import static com.mongodb.client.model.Filters.nin;
import com.mongodb.client.result.UpdateResult;

import entity.Time_sum;
import entity.Site;
import entity.Time_avg;

public class MongoDBJDBC {

	private static Mongo m = null;
	private static DB db = null;

	public static void conn() {
		try {
			// connect MongoDB
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase mongoDatabase = mongoClient.getDatabase("bigdata");

			Process process = null;
			String command1 = "/usr/local/mongodb/bin/mongoimport --db bigdata --collection sitelist --type csv --headerline --ignoreBlanks --file //Users/whisper/Documents/ubuntushar/all_sites.csv";
			process = Runtime.getRuntime().exec(command1);
			
			
			mongoDatabase.createCollection("csvlist");

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	// mongoimport is used to import allsite.csv and 100 csvs of each site to
	// the database.
	@SuppressWarnings("deprecation")
	public static void mongoimport() {
		try {
			// connect to mongodb
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase mongoDatabase = mongoClient.getDatabase("bigdata");

			MongoCollection<Document> collection = mongoDatabase.getCollection("sitelist");
			Process process = null;

			MongoCollection<Document> collection2 = mongoDatabase.getCollection("csvlist");

			//get the site id from collection sitelist, named companylist
			m = new Mongo("127.0.0.1", 27017);
			db = m.getDB("bigdata");
			DBCollection dbCol = db.getCollection("sitelist");
			DBCursor ret = dbCol.find();

			List<String> companylist = new ArrayList<String>();

			String company = null;
			long csvcount = 0;
			// insert 100 .csv
			while (ret.hasNext()) {
				BasicDBObject bdbObj = (BasicDBObject) ret.next();

				if (bdbObj != null) {

					System.out.println("SITE_ID:" + bdbObj.getString("SITE_ID"));
					company = bdbObj.getString("SITE_ID");

					// import allsite.csv
					String command2 = "/usr/local/mongodb/bin/mongoimport --db bigdata --collection csvlist --type csv --headerline --ignoreBlanks --file //Users/whisper/Documents/ubuntushar/"
							+ company + ".csv";

					process = Runtime.getRuntime().exec(command2);

					process.waitFor();

					// the count of csv
					long cnt = collection2.count();
					System.out.println("cnt=" + cnt);
					companylist.add(company);
					// add the id to new documents

					//System.out.println("company=" + company);
					UpdateResult updateResult = collection2.updateMany(nin("siteid", companylist),
							new Document("$set", new Document("siteid", company)));
					//System.out.println("update:" + updateResult.getModifiedCount());

				}
			}

		} catch (Exception e) {
			System.err.println("!!error:" + e.getClass().getName() + ": " + e.getMessage());
		}
	}

	public static String waitFor(Process p) throws IOException {
		String str = null;
		InputStream is1 = p.getInputStream();
		InputStream is2 = p.getErrorStream();
		BufferedReader br = new BufferedReader(new InputStreamReader(is1));
		while (true) {
			boolean exitFlag = true;
			if (is1.available() > 0) {
				Character c = new Character((char) is1.read());
				System.out.print(c);
				exitFlag = false;
			}
			if (is2.available() > 0) {
				Character c = new Character((char) is2.read());
				System.out.print(c);
				exitFlag = false;
			}
			if (exitFlag) {
				try {
					Thread.sleep(100); // Nothing to do, sleep a while...
					p.exitValue(); // ThrowIllegalThreadStateException, if the
									// subprocess represented by this Process
									// object has not yet terminated.
					break;
				} catch (InterruptedException ex) {
					ex.printStackTrace();
				} catch (IllegalThreadStateException ex) {
					// Process still alive
				}
			}
		}
		br.close();
		is1.close();
		p.destroy();
		return str;
	}

	// Calculate the sum LD for the 100 sites (timestamp interval: 5 minutes)
	public void sumLD5() {

		try {

			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase mongoDatabase = mongoClient.getDatabase("bigdata");
			MongoCollection<Document> collection = mongoDatabase.getCollection("csvlist");

			collection.createIndex(Indexes.ascending("timestamp", "siteid"));
			ArrayList<Time_sum> time_sum = new ArrayList<Time_sum>();

			// This method is used to put all output into arraylist in order to
			// do other steps like drawing a chart
			Block<Document> addTime_sum = new Block<Document>() {

				@Override
				public void apply(Document document) {
					// TODO Auto-generated method stub
					Time_sum ts = new Time_sum();
					String doc = document.toJson();
					String t = doc.substring(10, 20);
					String s = null;
					char s1[] = doc.toCharArray();
					int i = 0;
					for (i = 32; i < 44; i++) {
						if ((s1[i] == ' ') || (s1[i] == '}')) {
							break;
						}
					}

					s = doc.substring(32, i);

					System.out.println(t);
					System.out.println(s);
					ts.setTimestamp(t);
					ts.setSum(s);
					// System.out.println(document.toJson());
					time_sum.add(ts);
				}

			};

			collection.aggregate(Arrays.asList(
					Aggregates.group("$timestamp", Accumulators.sum("total", "$value")))).forEach(addTime_sum);

		} catch (Exception e) {
			System.err.println("!!error:" + e.getClass().getName() + ": " + e.getMessage());
		}
	}

	// Calculate the total LD for the 100 sites (timestamp interval: a week)
	public void sumLD7() {

		try {

			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase mongoDatabase = mongoClient.getDatabase("bigdata");
			MongoCollection<Document> collection = mongoDatabase.getCollection("csvlist");

			collection.createIndex(Indexes.ascending("timestamp", "siteid"));
			ArrayList<Time_sum> time_sum = new ArrayList<Time_sum>();

			// This method is used to put all output into arraylist in order to
			// do other steps like drawing a chart
			Block<Document> addTime_sum = new Block<Document>() {

				@Override
				public void apply(Document document) {
					// TODO Auto-generated method stub
					Time_sum ts = new Time_sum();
					String doc = document.toJson();
					String t = doc.substring(10, 20);
					String s = null;
					char s1[] = doc.toCharArray();
					int i = 0;
					for (i = 32; i < 44; i++) {
						if ((s1[i] == ' ') || (s1[i] == '}')) {
							break;
						}
					}

					s = doc.substring(32, i);

					System.out.println(t);
					System.out.println(s);
					ts.setTimestamp(t);
					ts.setSum(s);
					// System.out.println(document.toJson());
					time_sum.add(ts);
				}

			};

			// in order to calculate between 7 days, use timestamp to separate
			// the time
			int tstart = 13257600;
			int tend = 0;
			do {
				tend = tstart + 604800;
				String tss = String.valueOf(tstart);
				String tee = String.valueOf(tend);
				collection
						.aggregate(Arrays.asList(
								Aggregates.match(Filters.and(Filters.gte("_id", tss), Filters.lt("_id", tee))),
								Aggregates.group("$estimated", Accumulators.sum("total", "$value"))))
						.forEach(addTime_sum);

				tstart = tstart + 300;
			} while (tend <= 1356998400);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Calculate the average LD by sector of activity (timestamp interval: 5
	// minutes)
	@SuppressWarnings("deprecation")
	public void avgLD5() {

		try {

			@SuppressWarnings("resource")
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase mongoDatabase = mongoClient.getDatabase("bigdata");
			MongoCollection<Document> collection = mongoDatabase.getCollection("csvlist");
			ArrayList<Time_avg> time_avg = new ArrayList<Time_avg>();

			// list of 100 sites
			ArrayList<Site> sites = new ArrayList<Site>();
			Site site = new Site();

			m = new Mongo("127.0.0.1", 27017);

			db = m.getDB("bigdata");
			DBCollection dbCol = db.getCollection("sitelist");
			DBCursor ret = dbCol.find();

			while (ret.hasNext()) {
				BasicDBObject bdbObj = (BasicDBObject) ret.next();

				if (bdbObj != null) {

					site.setSiteid(bdbObj.getString("SITE_ID"));
					site.setIndustry(bdbObj.getString("INDUSTRY"));
					sites.add(site);
				}
			}

			String indnow = null;
			indnow = sites.get(0).getIndustry();
			// first siteid
			int start = 0;
			int end = 0;
			try {
				start = Integer.parseInt(sites.get(0).getSiteid());
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}

			// This method is used to put all output into arraylist in order to
			// do other steps like drawing a chart
			Block<Document> addTime_avg = new Block<Document>() {

				@Override
				public void apply(Document document) {
					// TODO Auto-generated method stub
					Time_avg ts = new Time_avg();
					String doc = document.toJson();
					// get id and values from the output of mongo
					String t = doc.substring(10, 20);
					String s = null;
					char s1[] = doc.toCharArray();

					int i = 0;
					for (i = 32; i < s1.length; i++) {
						if ((s1[i] == ' ') || (s1[i] == '}')) {
							break;
						}
					}

					s = doc.substring(32, i);

					System.out.println(t);
					System.out.println(s);
					ts.setIndustry(t);
					ts.setAvg(s);
					// System.out.println(document.toJson());
					time_avg.add(ts);
				}

			};

			for (int i = 0; i < 100; i++) {

				String ind = sites.get(i).getIndustry();
				if (ind.equals(indnow)) {
					continue;
				} else {
					// until now there are all one kinds of activity
					try {
						end = Integer.parseInt(sites.get(i).getSiteid());
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
					collection
							.aggregate(Arrays.asList(
									Aggregates.match(
											Filters.and(Filters.gte("siteid", start), Filters.lt("siteid", end))),
									Aggregates.group("$timestamp", Accumulators.avg("avg", "$value"))))
							.forEach(addTime_avg);
					indnow = sites.get(i).getIndustry();
					start = i + 1;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Calculate the average LD by sector of activity (timestamp interval: a
	// week)
	@SuppressWarnings("deprecation")
	public void avgLD7() {

		try {

			@SuppressWarnings("resource")
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase mongoDatabase = mongoClient.getDatabase("bigdata");
			MongoCollection<Document> collection = mongoDatabase.getCollection("csvlist");
			ArrayList<Time_avg> time_avg = new ArrayList<Time_avg>();

			// list of 100 sites
			ArrayList<Site> sites = new ArrayList<Site>();
			Site site = new Site();

			m = new Mongo("127.0.0.1", 27017);

			db = m.getDB("bigdata");
			DBCollection dbCol = db.getCollection("sitelist");
			DBCursor ret = dbCol.find();

			// the list of industry
			while (ret.hasNext()) {
				BasicDBObject bdbObj = (BasicDBObject) ret.next();

				if (bdbObj != null) {

					site.setSiteid(bdbObj.getString("SITE_ID"));
					site.setIndustry(bdbObj.getString("INDUSTRY"));
					sites.add(site);
				}
			}

			String indnow = null;
			indnow = sites.get(0).getIndustry();
			int start = 0;
			int end = 0;
			try {
				start = Integer.parseInt(sites.get(0).getSiteid());
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}

			// This method is used to put all output into arraylist in order to
			// do other steps like drawing a chart
			Block<Document> addTime_avg = new Block<Document>() {

				@Override
				public void apply(Document document) {
					// TODO Auto-generated method stub
					Time_avg ts = new Time_avg();
					String doc = document.toJson();
					// get id and values from the output of mongo
					String t = doc.substring(10, 20);
					String s = null;
					char s1[] = doc.toCharArray();
					int i = 0;
					for (i = 32; i < s1.length; i++) {
						if ((s1[i] == ' ') || (s1[i] == '}')) {
							break;
						}
					}

					s = doc.substring(32, i);

					System.out.println(t);
					System.out.println(s);
					ts.setIndustry(t);
					ts.setAvg(s);
					// System.out.println(document.toJson());
					time_avg.add(ts);
				}

			};

			for (int i = 0; i < 100; i++) {

				String ind = sites.get(i).getIndustry();
				if (ind.equals(indnow)) {
					continue;
				} else {
					try {
						end = Integer.parseInt(sites.get(i).getSiteid());
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
					collection
							.aggregate(Arrays.asList(
									Aggregates.match(
											Filters.and(Filters.gte("siteid", start), Filters.lt("siteid", end))),
									Aggregates.group("$timestamp", Accumulators.sum("total", "$value"))))
							.forEach(addTime_avg);

					int tstart = 13257600;
					int tend = 0;
					do {
						tend = tstart + 604800;
						String tss = String.valueOf(tstart);
						String tee = String.valueOf(tend);
						collection
								.aggregate(Arrays.asList(
										Aggregates.match(Filters.and(Filters.gte("_id", tss), Filters.lt("_id", tee),
												Filters.lt("siteid", end), Filters.gte("siteid", start))),
										Aggregates.group("$estimated", Accumulators.sum("total", "$value"))))
								.forEach(addTime_avg);

						tstart = tstart + 300;
					} while (tend <= 1356998400);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
