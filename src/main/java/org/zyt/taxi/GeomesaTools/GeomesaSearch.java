package org.zyt.geomesa;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.service.Service;
import org.geotools.data.*;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.sort.SortBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class GeomesaSearch {
    public static String typeName = "taxi-geomesa";
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

    private final Map<String, String> params;
    private final TutorialData data;

    public GeomesaSearch(String[] args, DataAccessFactory.Param[] parameters, TutorialData data) throws ParseException {
        // parse the data store parameters from the command line
        Options options = createOptions(parameters);
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, args);
        params = CommandLineDataStore.getDataStoreParams(command, options);
        this.data = data;
        initializeFromOptions(command);
    }
    public ByteArrayOutputStream searchRoute(String startTime, String endTime, String carID,
                                             String startAreaString, String endAreaString, String maxView) throws IOException {
        String queryCQL = getECQL(startTime, endTime, carID, startAreaString, endAreaString, maxView);
        System.out.println(queryCQL);

        ByteArrayOutputStream result = null;
        List<SimpleFeature> queryResult = null;
        DataStore datastore = null;
        try {
            datastore = createDataStore(params);
            SimpleFeatureType sft = getSimpleFeatureType(data);
            createSchema(datastore, sft);

            Query query = new Query(typeName, ECQL.toFilter(queryCQL));
            queryResult = queryFeatures(datastore, query);
            // Limit max number result
            queryResult = queryResult.subList(0, Math.min(queryResult.size(), Integer.parseInt(maxView)));

            result = featuresToByteArray(queryResult);
        } catch (CQLException e) {
            throw new RuntimeException("Error creating filter:", e);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }


    public String getECQL(String startTime, String endTime, String carID,
                          String startAreaString, String endAreaString, String maxView) {
        String queryCQL = "";
        if (startTime!=null && !startTime.equals("")) {
            String startTimeF = sdf.format(Long.parseLong(startTime) * 1000).replace("+0800", "Z");
            queryCQL = queryCQL + "endTime >= '" + startTimeF + "' AND ";
        }
        if (endTime!=null && !endTime.equals("")) {
            String endTimeF = sdf.format(Long.parseLong(endTime) * 1000).replace("+0800", "Z");
            queryCQL = queryCQL + "startTime <= '" + endTimeF + "' AND ";
        }
        if (carID!=null && !carID.equals("")) {
            queryCQL = queryCQL + "carID = '" + carID + "' AND ";
        }
        if (startAreaString!=null && !startAreaString.equals("")) {
            queryCQL = queryCQL + "CONTAINS(Polygon((" + polygonFormat(startAreaString) + ")), startPoint) AND ";
        }
        if (endAreaString!=null && !endAreaString.equals("")) {
            queryCQL = queryCQL + "CONTAINS(Polygon((" + polygonFormat(endAreaString) + ")), startPoint) AND ";
        }
        queryCQL = queryCQL.substring(0, queryCQL.length() - 4); // remove the last AND
        return queryCQL;
    }

    public String polygonFormat(String s) {
        String res = "";
        StringBuilder resBuilder = new StringBuilder();
        if (s != "" && s != null) {
            String list1[] = s.split(";");
            for (String pointStr : list1) {
                double x = Double.parseDouble(pointStr.split(",")[0]);
                double y = Double.parseDouble(pointStr.split(",")[1]);
                resBuilder.append(x);
                resBuilder.append(" ");
                resBuilder.append(y);
                resBuilder.append(",");
            }
            res = resBuilder.toString();
            res = res.substring(0, res.length() - 1);
        }
        return res;
    }

    public List<SimpleFeature> queryFeatures(DataStore datastore, Query query) throws IOException {
        List<SimpleFeature> queryFeatureList = new ArrayList<>();

        System.out.println("Running query " + ECQL.toCQL(query.getFilter()));
        if (query.getPropertyNames() != null) {
            System.out.println("Returning attributes " + Arrays.asList(query.getPropertyNames()));
        }
        if (query.getSortBy() != null) {
            SortBy sort = query.getSortBy()[0];
            System.out.println("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
        }
        // submit the query, and get back an iterator over matching features
        // use try-with-resources to ensure the reader is closed
        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                     datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
            int n = 0;
            while (reader.hasNext()) {
                SimpleFeature feature = reader.next();
                queryFeatureList.add(feature);
                n++;
            }
            System.out.println();
            System.out.println("Returned " + n + " total features");
            System.out.println();
        }

        return queryFeatureList;
    }
    public Options createOptions(DataAccessFactory.Param[] parameters) {
        // parse the data store parameters from the command line
        Options options = CommandLineDataStore.createOptions(parameters);

        options.addOption(Option.builder().longOpt("cleanup").desc("Delete tables after running").build());

        return options;
    }

    public DataStore createDataStore(Map<String, String> params) throws IOException {
        System.out.println("Creating data store");

        // use geotools service loading to get a datastore instance
        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        System.out.println();
        return datastore;
    }

    public void initializeFromOptions(CommandLine command) {
    }

    public SimpleFeatureType getSimpleFeatureType(TutorialData data) {
        return data.getSimpleFeatureType();
    }

    public void createSchema(DataStore datastore, SimpleFeatureType sft) throws IOException {
        System.out.println("Creating schema: " + DataUtilities.encodeType(sft));
        // we only need to do the once - however, calling it repeatedly is a no-op
        datastore.createSchema(sft);
        System.out.println();
    }

    public ByteArrayOutputStream featuresToByteArray(List<SimpleFeature> queryResult) throws IOException, java.text.ParseException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (SimpleFeature feature : queryResult) {
            String carID = feature.getProperty("carID").getValue().toString();
            String afterLength  = feature.getProperty("afterLength").getValue().toString();
            String avgSpeed = feature.getProperty("avgSpeed").getValue().toString();
            String tpList = feature.getProperty("tpList").getValue().toString();
            String startTime = String.valueOf(((Date) feature.getProperty("startTime").getValue()).getTime() / 1000);
            String endTime = String.valueOf(((Date) feature.getProperty("endTime").getValue()).getTime() / 1000);

            byte[] bytes = carID.getBytes();
            baos.write(bytes);

            baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(startTime))));
            baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(endTime))));
            baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(avgSpeed))));
            baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(afterLength))));

            String[] tpItems = tpList.split(",");

            for (int i=0;i<tpItems.length; i++){
                baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(tpItems[i]))));
            }
        }
        return baos;
    }
    public static byte[] getBytes(int data) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (data & 0xff);
        bytes[1] = (byte) ((data & 0xff00) >> 8);
        bytes[2] = (byte) ((data & 0xff0000) >> 16);
        bytes[3] = (byte) ((data & 0xff000000) >> 24);
        return bytes;
    }

    public static void main(String[] args) throws IOException {
        try {
            args = new String[]{"--hbase.catalog", "a", "--hbase.zookeepers", "192.168.1.133"};
            new GeomesaSearch(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
                    .searchRoute("1391124280", "1391124400",
                            null,
                            "114.264,30.710;114.266,30.710;114.266,30.712;114.264,30.712;114.264,30.710",
                            null,
                            "10");
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
