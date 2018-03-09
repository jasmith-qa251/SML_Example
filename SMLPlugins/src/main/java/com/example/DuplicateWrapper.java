package com.example;

import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.api.spark.sql.DataFrames;

import com.example.api.java.methods.JavaDuplicateFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;


import java.util.*;

/**
 * @author Saul Pountney
 *
 * This is a CDAP plugin Wrapper which uses the duplicate marker function refencing a java factory
 * from the SML.
 */

@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(DuplicateWrapper.NAME)
@Description("This plugin will create a duplicate marker based on a given dataframe.")
public class DuplicateWrapper extends SparkCompute<StructuredRecord, StructuredRecord> {
    public static final String NAME = "Duplicate_Wrapper";
    private final Conf config;

    public DuplicateWrapper(Conf config) {
        this.config = config;
    }

    /**
     * This function configures the pipeline and sets the output schema, the function below
     * adds a new column to the output schema which will contain the marker information.
     *
     * @param pipelineConfigurer
     */
    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
        ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>(inputSchema.getFields());
        fields.add(Schema.Field.of(config.new_col, Schema.of(Schema.Type.INT)));
        Schema outputSchema = Schema.recordOf(DuplicateWrapper.NAME, fields);
        pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    }

    @Override
    public void initialize(SparkExecutionPluginContext context) throws Exception {
        super.initialize(context);
    }

    /**
     * This methord collects data and inputs it into a schema, this input schema then applies the duplicate marker
     * function and returns an output dataframe.
     *
     * @param sparkExecutionPluginContext
     * @param javaRDD
     * @return dupDF
     * @throws Exception
     */
    @Override
    public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext sparkExecutionPluginContext,
                                               JavaRDD<StructuredRecord> javaRDD) throws Exception {

        HiveContext hiveCon = new HiveContext(sparkExecutionPluginContext.getSparkContext());
        final StructType dfSchema = DataFrames.toDataType(sparkExecutionPluginContext.getInputSchema());
        JavaRDD<Row> rowRDD = javaRDD.map((StructuredRecord r) -> DataFrames.toRow(r, dfSchema));
        DataFrame dataframe = hiveCon.createDataFrame(rowRDD, dfSchema);
        DataFrame dupDF = JavaDuplicateFactory.duplicate(dataframe).dm1(dataframe, ListConversion(config.partCol),
                ListConversion(config.ordCol),config.new_col);
        Schema outputSchema = DataFrames.toSchema(dupDF.schema());
        return dupDF.javaRDD().map((Row r) -> DataFrames.fromRow(r, outputSchema));
    }

    private static ArrayList<String> ListConversion(String columnNames){

        ArrayList<String> List = new ArrayList<String>(Arrays.asList(columnNames.split(",")));

        return List;
    }

    public static class Conf extends PluginConfig {

        @Macro
        @Description("Columns that are used to Partition the window")
        private final String partCol;
        @Description("Columns used to order the window")
        private final String ordCol;
        @Description("Marker Column")
        private final String new_col;

        /**
         *  This is the configuration setting for the input schema.
         *
         * @param partitionColumns String    - List of columns to partition.
         * @param orderColumns  String       - List of columns to order from.
         * @param new_col String             - Marked Duplicates.
         */
        public Conf(String partitionColumns, String orderColumns, String new_col) {
            this.partCol = partitionColumns;
            this.ordCol = orderColumns;
            this.new_col = new_col;
        }
    }
}


