package com.example.api.java.methods;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public final class JavaDuplicateFactory {

    private static final JavaDuplicate$ JAVA_DUPLICATE = JavaDuplicate$.MODULE$;

    private JavaDuplicateFactory() {}

    public static JavaDuplicate duplicate(DataFrame df){
        return JAVA_DUPLICATE.duplicate(df);}
}

