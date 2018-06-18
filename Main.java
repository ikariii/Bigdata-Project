
public class Main {

	@SuppressWarnings("static-access")
	public static void main(String args[]) {
		MongoDBJDBC mongodb = new MongoDBJDBC();
		// connect the mongodb
		mongodb.conn();

		// import the allsite.csv
		mongodb.mongoimport();

		// Calculate the sum LD for the 100 sites (timestamp interval: 5
		// minutes)
		mongodb.sumLD5();
		// Calculate the total LD for the 100 sites (timestamp interval: a week)
		mongodb.sumLD7();
		// Calculate the average LD by sector of activity (timestamp interval: 5
		// minutes)
		mongodb.avgLD5();
		// Calculate the average LD by sector of activity (timestamp interval: a
		// week)
		mongodb.avgLD7();
	}

}
