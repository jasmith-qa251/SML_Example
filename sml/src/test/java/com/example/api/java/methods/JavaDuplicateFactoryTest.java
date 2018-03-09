package com.example.api.java.methods;


import org.apache.spark.sql.DataFrame;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class JavaDuplicateFactoryTest {

    @ClassRule
    public static SparkCtxtProvider spark = SparkCtxtProvider.getTestResources();

    @Test
    public void duplicateTest() {

        // Input data
        String inData = "./src/test/resources/inputs/DuplicateMarker.json";
        //Dataset<Row> inDf = spark.sparkS().read().json(inData).select("id", "num", "order");
        DataFrame inDf = spark.hc().read().json(inData).select("id", "num", "order");
        System.out.println("Input DataFrame");
        inDf.show();

        // Expected data
        String expData = "./src/test/resources/outputs/DuplicateMarker.json";
        //DataFrame expDf = spark.sparkS().read().json(expData).select("id", "num", "order", "marker");
        DataFrame expDf = spark.hc().read().json(expData).select("id", "num", "order", "marker");
        System.out.println("Expected DataFrame");
        expDf.show();


        // Create Class
        JavaDuplicate Dup = JavaDuplicate.duplicate(inDf);

        // Output data
        ArrayList<String> partCol = new ArrayList<String>();
        ArrayList<String> ordCol = new ArrayList<String>();

        partCol.add("id");
        partCol.add("num");
        ordCol.add("order");

        //DataFrame outDf = Dup.dm1(inDf, partCol, ordCol, "marker");
        DataFrame outDf = Dup.dm1(inDf, partCol, ordCol, "marker").orderBy("id","num");
        System.out.println("Output DataFrame");
        outDf.show();

        // Assert
        assertEquals(expDf.collectAsList(), outDf.collectAsList());
    }
}