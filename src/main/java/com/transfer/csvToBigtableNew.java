package com.transfer;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import java.io.BufferedWriter;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;

public class csvToBigtableNew implements HttpFunction {
  @Override
  public void service(HttpRequest request, HttpResponse response) throws Exception {
    BufferedWriter writer = response.getWriter();
    setupEnv();
    writer.write("Import Done succesfully");
  }

  public  void setupEnv() throws Exception {
    String datasetName = "iShare_test_DataSet";
    String tableName = "csvDataTable";
    String sourceUri = "gs://isharetestbucket/t2.csv";
    loadCsvFromGcs(datasetName, tableName, sourceUri);
  }

  public Schema setSchemaFortheImport(){
    Schema schema =
        Schema.of(
          /**Field.of("name", StandardSQLTypeName.INT64),
          Field.of("place", StandardSQLTypeName.INT64),
          Field.of("number", StandardSQLTypeName.INT64)*/
            Field.of("EmployeeId", StandardSQLTypeName.STRING),
            Field.of("FirstName", StandardSQLTypeName.STRING),
            Field.of("LastName", StandardSQLTypeName.STRING),
            Field.of("ExternalEmployeeId", StandardSQLTypeName.STRING),
            Field.of("Gender", StandardSQLTypeName.INT64),
            Field.of("BirthDate", StandardSQLTypeName.STRING),
            Field.of("NationalityCode", StandardSQLTypeName.STRING),
            Field.of("StartDateInIngka", StandardSQLTypeName.STRING),
            Field.of("EmployeeStatus", StandardSQLTypeName.STRING),
            Field.of("EmploymentGroupCode", StandardSQLTypeName.STRING),
            Field.of("EmploymentGroupName", StandardSQLTypeName.STRING),
            Field.of("SubEmploymentGroupCode", StandardSQLTypeName.INT64),
            Field.of("EmailAddress", StandardSQLTypeName.STRING),
            Field.of("NetworkId", StandardSQLTypeName.INT64),
            Field.of("CountryColumn", StandardSQLTypeName.STRING),
            Field.of("CompanyName", StandardSQLTypeName.STRING),
            Field.of("CBDCompanyCode", StandardSQLTypeName.STRING),
            Field.of("BusinessUnitType", StandardSQLTypeName.STRING),
            Field.of("BusinessUnitCode", StandardSQLTypeName.STRING),
            Field.of("BusinessUnitName", StandardSQLTypeName.STRING),
            Field.of("CCFunction", StandardSQLTypeName.STRING),
            Field.of("OrgUnitCode", StandardSQLTypeName.STRING),
            Field.of("DepartmentName", StandardSQLTypeName.STRING),
            Field.of("CostCentre", StandardSQLTypeName.INT64),
            Field.of("JobTitle", StandardSQLTypeName.STRING),
            Field.of("ManagerEmployeeId", StandardSQLTypeName.STRING),
            Field.of("ManagerLastName", StandardSQLTypeName.STRING),
            Field.of("ManagerFirstName", StandardSQLTypeName.STRING),
            Field.of("ManagerFlag", StandardSQLTypeName.STRING),
            Field.of("ManagerEmailAddress", StandardSQLTypeName.STRING),
            Field.of("Level_01", StandardSQLTypeName.STRING),
            Field.of("Level_02", StandardSQLTypeName.STRING),
            Field.of("Level_04", StandardSQLTypeName.STRING),
            Field.of("ContractType", StandardSQLTypeName.STRING),
            Field.of("OrgKey", StandardSQLTypeName.STRING),
            Field.of("SubDepartment", StandardSQLTypeName.INT64),
            Field.of("PreferedLanguage", StandardSQLTypeName.INT64),
            Field.of("Level_03", StandardSQLTypeName.STRING),
            Field.of("Level_05", StandardSQLTypeName.STRING),
             Field.of("SubEmploymentGroupName", StandardSQLTypeName.INT64)
       );
       return schema;
  }

  public void loadCsvFromGcs(
      String datasetName, String tableName, String sourceUri) {
    try {
      Schema schema=setSchemaFortheImport();
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      // Skip header row in the file.
      CsvOptions csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(1).build();

      TableId tableId = TableId.of(datasetName, tableName);
      LoadJobConfiguration loadConfig =
          LoadJobConfiguration.newBuilder(tableId, sourceUri, csvOptions)
          .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
          .setSchema(schema)
          .build();

      // Load data from a GCS CSV file into the table
      Job job = bigquery.create(JobInfo.of(loadConfig));
      // Blocks until this load table job completes its execution, either failing or succeeding.
      job = job.waitFor();
      if (job.isDone()) {
        System.out.println("CSV from GCS successfully added during load append job");
      } else {
        System.out.println(
            "BigQuery was unable to load into the table due to an error:"
                + job.getStatus().getError());
      }
    } catch (BigQueryException | InterruptedException e) {
      System.out.println("Column not added during load append \n" + e.toString());
    }
  }

  
}
