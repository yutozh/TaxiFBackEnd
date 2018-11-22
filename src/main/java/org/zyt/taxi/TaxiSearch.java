package org.zyt.taxi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.geo.GeoPoint;
import org.zyt.taxi.ESTools.TaxiElasticSearch;
import org.zyt.taxi.HBTools.TaxiHbase;
import org.zyt.taxi.Utils.RecordTime;

public class TaxiSearch {

	public static ByteArrayOutputStream searchRoute(String startTime, String endTime, String carID, String startAreaString, String endAreaString, String maxView) throws IOException {

		List<GeoPoint> s1 = new ArrayList<GeoPoint>();
		List<GeoPoint> s2 = new ArrayList<GeoPoint>();
		String hasSA = "";
		String hasEA = "";
		if(startAreaString != "" && startAreaString != null){
			hasSA = "sa";
			String list1[] = startAreaString.split(";");
			for (String pointStr : list1) {
				double x = Double.parseDouble(pointStr.split(",")[0]);
				double y = Double.parseDouble(pointStr.split(",")[1]);
				s1.add(new GeoPoint(x, y));
			}
		}else {
			s1 = null;
		}
		if(endAreaString != "" && endAreaString != null){
			hasEA = "ea";
			String list2[] = endAreaString.split(";");
			for (String pointStr : list2) {
				double x = Double.parseDouble(pointStr.split(",")[0]);
				double y = Double.parseDouble(pointStr.split(",")[1]);
				s2.add(new GeoPoint(x, y));
			}
		}else {
			s2 = null;
		}



		long time1=System.currentTimeMillis();
		List<String> resES = TaxiElasticSearch.searchIndexTaxiRoute(startTime, endTime, carID, s1, s2, maxView);
		long time2=System.currentTimeMillis();
		List<String> resHb = new ArrayList<String>();
		
		// 字节流 LenthofTotalCar + CarList + pathList
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ByteArrayOutputStream resBaos = new ByteArrayOutputStream();

		System.out.println(resES.size());
		resBaos.write(getBytes(resES.size()));
		List<String> carIDList = new ArrayList<String>();
		TaxiHbase.init();
		for(String s : resES){
//			resHb.add(TaxiHbase.getData("taxi-route", s, "info", null));
			String listString[] = TaxiHbase.getData("taxi-route", s, "info", null).split(",");
			carIDList.add(listString[0]);
			for(int i=1; i<listString.length; i++){
				baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(listString[i]))));
			}
		}
		for (String car: carIDList){
			byte[] bytes = car.getBytes();
			resBaos.write(bytes);
		}

		long time3=System.currentTimeMillis();
		RecordTime.writeLocalStrOne("Search "+(time2-time1) + " " +
				(time3-time2) + " " +
				(time3-time1) + " " +
				startTime  + " " +
				endTime + " " +
				carID + " " +
				hasSA  + " " +
				hasEA + " " +
				maxView + " " +
				resES.size() + "\n" , "");

		resBaos.write(baos.toByteArray());
		TaxiHbase.close();



//		String resString = "{\"result\":" + resHb.toString() + "}";
//		System.out.println(resString);
		return resBaos;
	}
    public static byte[] getBytes(int data)  
    {  
        byte[] bytes = new byte[4];  
        bytes[0] = (byte) (data & 0xff);  
        bytes[1] = (byte) ((data & 0xff00) >> 8);  
        bytes[2] = (byte) ((data & 0xff0000) >> 16);  
        bytes[3] = (byte) ((data & 0xff000000) >> 24);  
        return bytes;  
    }  
	public static void main(String[] args) throws IOException{
//		searchRoute("","1391110838",null,null,null,"");
//		String[] queryArgs = new String[]{"1391097600","1397491200","MMC8000GPSANDASYN051113-24031-00000000",
//				"30.699998413819344,114.27431088282236;30.66435133685907,114.2250918328665;30.64645588596933,114.16284328303868;30.600641310705427,114.14425454049046;30.55514216961467,114.15615774991699;30.526901799773015,114.21209772585422;30.49581165146344,114.24200154621985;30.48704896697609,114.28111915983226;30.47993151370619,114.31930093802578;30.494334009970245,114.36866576479298;30.522073047341294,114.38442634299878;30.548282230864558,114.38911547800978;30.574826727850024,114.38776149704336;30.598135889247985,114.37489154828143;30.611068468313476,114.35999724045945;30.629526232052896,114.35278738406373;30.65509279752601,114.37182499890577;30.678295584750188,114.35844206679155;30.687458411581588,114.33438982600961;30.677472747372846,114.3137331462425;30.67747274735173,114.31373314628206;30.66772743312417,114.27469060530387;30.699998413819344,114.27431088282236",
//				"30.699998413819344,114.27431088282236;30.66435133685907,114.2250918328665;30.64645588596933,114.16284328303868;30.600641310705427,114.14425454049046;30.55514216961467,114.15615774991699;30.526901799773015,114.21209772585422;30.49581165146344,114.24200154621985;30.48704896697609,114.28111915983226;30.47993151370619,114.31930093802578;30.494334009970245,114.36866576479298;30.522073047341294,114.38442634299878;30.548282230864558,114.38911547800978;30.574826727850024,114.38776149704336;30.598135889247985,114.37489154828143;30.611068468313476,114.35999724045945;30.629526232052896,114.35278738406373;30.65509279752601,114.37182499890577;30.678295584750188,114.35844206679155;30.687458411581588,114.33438982600961;30.677472747372846,114.3137331462425;30.67747274735173,114.31373314628206;30.66772743312417,114.27469060530387;30.699998413819344,114.27431088282236"};
		String[] queryArgs = new String[]{"1393603200","1394812800","MMC8000GPSANDASYN051113-24031-00000000",
				"30.63530499158118,114.2747493313646;30.603159551261527,114.22370746587717;30.5657007520701,114.2181404891204;30.53724232418722,114.25797708273006;30.572577763852564,114.28439504838872;30.614571204282683,114.31175739238586;30.63530499158118,114.2747493313646",
				"30.63530499158118,114.2747493313646;30.603159551261527,114.22370746587717;30.5657007520701,114.2181404891204;30.53724232418722,114.25797708273006;30.572577763852564,114.28439504838872;30.614571204282683,114.31175739238586;30.63530499158118,114.2747493313646"
		};

		int[] times = new int[]{100,2500,5000,7500,10000};
		String[] endtime = new String[]{"1394553600","1394899200","1395590400","1396972800","1402243200"};
		for (int i=0;i<=4;i++){
			for(int j=0;j<=4;j++){
				searchRoute(queryArgs[0],endtime[i],"","","",String.valueOf(times[j]));
				System.out.print("=");
			}
		}
//		for (int i=0;i<=4;i++){
//			for(int j=0;j<4;j++){
//				searchRoute(queryArgs[0],"","","","",String.valueOf(times[i]));
//				System.out.print("=");
//			}
//		}
//		System.out.println();
//		for (int i=0;i<=4;i++){
//			for(int j=0;j<4;j++){
//				searchRoute("",queryArgs[1],"","","",String.valueOf(times[i]));
//				System.out.print("=");
//			}
//		}
//		for (int i=0;i<=4;i++){
//			for(int j=0;j<4;j++){
//				searchRoute("","",queryArgs[2],"","",String.valueOf(times[i]));
//				System.out.print("=");
//			}
//		}
//		System.out.println();
//		for (int i=0;i<=4;i++){
//			for(int j=0;j<4;j++){
//				searchRoute("","","",queryArgs[3],"",String.valueOf(times[i]));
//				System.out.print("=");
//			}
//		}
//		System.out.println();
//		for (int i=0;i<=4;i++){
//			for(int j=0;j<4;j++){
//				searchRoute("","","","",queryArgs[4],String.valueOf(times[i]));
//				System.out.print("=");
//			}
//		}
//		System.out.println();
//		for (int i=0;i<=4;i++){
//			for(int j=0;j<4;j++){
//				searchRoute("","","",queryArgs[3],queryArgs[4],String.valueOf(times[i]));
//				System.out.print("=");
//			}
//		}
//		System.out.println();
//		for (int i=0;i<=4;i++){
//			for(int j=0;j<4;j++){
//				searchRoute(queryArgs[0],queryArgs[1],"",queryArgs[3],queryArgs[4],String.valueOf(times[i]));
//				System.out.print("=");
//			}
//		}

		System.out.println();
	}
}
