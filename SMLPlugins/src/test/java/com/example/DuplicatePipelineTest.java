package com.example;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.format.io.JsonDecoder;
import co.cask.cdap.format.io.JsonStructuredRecordDatumReader;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import com.google.gson.stream.JsonReader;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.FileReader;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for the plugins
 */
public class DuplicatePipelineTest extends HydratorTestBase {
    private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("sml-pipeline", "1.0.0");
    @ClassRule
    public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);
    @BeforeClass
    public static void setupTestClass() throws Exception {
        ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

        // Add the data-pipeline artifact and plugins
        setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

        // This adds our plugins to the parent artifact and allows them to be available to the data-pipeline
        addPluginArtifact(NamespaceId.DEFAULT.artifact("sml-plugins", "1.0.0"),
                parentArtifact,
                DuplicateWrapper.class);
    }

    /**
     * This function reads json from a file for use in the CDAP tests
     *
     * The Json must be an array of objects e.g.
     * [
     *  {"id":1, "num":4, "order":1},
     *  {"id":1, "num":4, "order":2}
     * ]
     *
     * @param path String              - The path to the json file
     * @param schema Schema            - The schema of the data
     * @return List<StructuredRecord>  - The data in structured record format
     * @throws Exception               - Generic exception
     */
    private static List<StructuredRecord> readJson(String path, Schema schema) throws Exception {
        // Get the JSON
        FileReader fileReader = new FileReader(path);
        JsonReader jsonReader = new JsonReader(fileReader);
        jsonReader.beginArray();

        // Create the structured record array
        JsonStructuredRecordDatumReader datumReader = new JsonStructuredRecordDatumReader();
        List<StructuredRecord> records = new ArrayList<>();

        while (jsonReader.hasNext()) {
            JsonDecoder jsonDecoder = new JsonDecoder(jsonReader);
            records.add(datumReader.read(jsonDecoder, schema));
        }

        return records;
    }

    @Test
    public void testDuplicate() throws Exception {
        // Defining the table names
        String inputName = "DuplicateInput";
        String outputName = "DuplicateTestOutput";

        // Creating the input Schema
        Schema inputSchema = Schema.recordOf(
                "Data",
                Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                Schema.Field.of("num", Schema.of(Schema.Type.INT)),
                Schema.Field.of("order", Schema.of(Schema.Type.INT))
        );

        // Defining the source stage,  this uses a mock source and needs the input data name and the
        // input data's schema
        ETLStage source = new ETLStage("sourceStage", MockSource.getPlugin(inputName, inputSchema));

        // These properties are the input parameters of the plugin thats been tested.
        Map<String, String> transformProperties = new HashMap<>();
        transformProperties.put("partCol", "num");
        transformProperties.put("ordCol", "Order");
        transformProperties.put("new_col", "Marker");
        // This sets the stage for our Standard Error plugin, you'll need the name of the class, the type of
        // plugin it is, as well as any properties you need to set. For the purpose of this the artifact is null
        // this is what it is in all the examples CDAP show us.
        ETLStage transform = new ETLStage("DuplicateStage",
                new ETLPlugin(DuplicateWrapper.NAME, SparkCompute.PLUGIN_TYPE, transformProperties, null));

        // This sets the stage for the output data to come out of, we are using a mock sink plugin as we
        // testing the format of how the data comes out.
        ETLStage sink = new ETLStage("sinkStage", MockSink.getPlugin(outputName));

        // Batch config
        //  This aligns all the different stages together, each of them will need to be add through the .addStage
        // You then need to set up connections of each one, eg. the source is first then is followed by the transform,
        // it is the lines inbetween the blocks on a pipeline.
        ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
                .addStage(source)
                .addStage(transform)
                .addStage(sink)
                .addConnection(source.getName(), transform.getName())
                .addConnection(transform.getName(), sink.getName())
                .build();

        // Here the pipeline id is created with a name in a given namespace
        ApplicationId pipelineId = NamespaceId.DEFAULT.app("DuplicatePipeline");
        // Here we are deploying our pipeline, this doesn't run the pipeline and each pipeline needs to be
        // deployed before it can be run. This is creates the pipeline from the id variable above the pipelineConfig
        // also set above.
        // This bit of code goes into the configurePipeline function of the Standard Error wrapper
        ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));

        // Creating the input data
        DataSetManager<Table> inputManager = getDataset(inputName);
        // This creates an new/empty array list to hold all the values/data in
        List<StructuredRecord> inputRecords = readJson(".\\src\\test\\Resources\\in\\DUPINDATA.json", inputSchema);
        // This writes the input data to the mock source outlined in the source stage, the inputName links
        // these together
        MockSource.writeInput(inputManager, inputRecords);

        // These three lines of code starts the process of the pipeline
        WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
        workflowManager.start();
        workflowManager.waitForFinish(4, TimeUnit.MINUTES);

        // This line gets the output dataset
        DataSetManager<Table> outputManager = getDataset(outputName);
        // This creates a set for the output records to be deposited into
        Set<StructuredRecord> outputRecords = new HashSet<>();
        // This add all the output records to our set
        outputRecords.addAll(MockSink.readOutput(outputManager));

        // Creating the output schema for the expected data to use
        Schema outputSchema = Schema.recordOf(
                "Data",
                Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                Schema.Field.of("num", Schema.of(Schema.Type.INT)),
                Schema.Field.of("order", Schema.of(Schema.Type.INT)),
                Schema.Field.of("Marker", Schema.of(Schema.Type.INT))
        );

        // This creates a set with the variable name expected for the expected record to go into
        Set<StructuredRecord> expected = new HashSet<>(readJson(".\\src\\test\\Resources\\out\\DUPExpected.json", outputSchema));

        // Asserts that the expected data is equal to the data we receive out of the pipeline.
        // If it fails it will appear as two structure records, use the debugging tool on the line
        /// this will let you see the actual data and schema
        Assert.assertEquals(expected, outputRecords);
    }
}