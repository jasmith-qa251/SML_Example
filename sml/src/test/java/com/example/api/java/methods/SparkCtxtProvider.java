package com.example.api.java.methods;


import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.rules.ExternalResource;
import org.apache.spark.sql.hive.test.TestHiveContext;

import java.io.File;
import java.io.IOException;

public class SparkCtxtProvider extends ExternalResource {
    private static int refCount = 0;

    private static SparkCtxtProvider currentInstance;
    private static transient SparkContext _sc;
    private static transient TestHiveContext _hc;

    public SparkContext sc() {
        return _sc;
    }

    public TestHiveContext hc() {
        return _hc;
    }

    public static SparkCtxtProvider getTestResources() {
        if (refCount == 0) {
            // currentInstance either hasn't been created yet, or after was called on it - create a new one
            currentInstance = new SparkCtxtProvider();
        }
        return currentInstance;
    }

    private SparkCtxtProvider() {
    }

    protected void before() {
        try {
            if (refCount == 0) {
                SparkConf conf = new SparkConf()
                        .setMaster("local[*]")
                        .setAppName("test")
                        .set("spark.ui.enabled", "false")
                        .set("spark.driver.host", "localhost");
                _sc = new SparkContext(conf);
                _sc.setLogLevel("ERROR");
                _hc = new TestHiveContext(_sc);
            }
        } finally {
            refCount++;
        }
    }

    protected void after() {
        refCount--;
        if (refCount == 0) {
            _hc = null;
            _sc.stop();
            _sc = null;

            File hiveLocalMetaStorePath = new File("metastore_db");
            try {
                FileUtils.deleteDirectory(hiveLocalMetaStorePath);
            } catch (IOException e) {
              e.printStackTrace();
            }
        }
    }
}