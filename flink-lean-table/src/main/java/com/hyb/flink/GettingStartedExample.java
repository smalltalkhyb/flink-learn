package com.hyb.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.range;
import static org.apache.flink.table.api.Expressions.withColumns;

/**
 * @program: flink-learn
 * @description:
 * @author: huyanbing
 * @create: 2025-05-24
 **/
public final class GettingStartedExample {

    public static void main(String[] args) throws Exception {


        // 解析命令行参数
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        Map<String, String> map = parameterTool.toMap();

        for (Map.Entry<String, String> entry : map.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }


        ObjectMapper objectMapper = new ObjectMapper();


        // setup the unified API
        // in this case: declare that the table programs should be executed in batch mode
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        final TableEnvironment env = TableEnvironment.create(settings);

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment();

        String params_json = objectMapper.writeValueAsString(parameterTool.toMap());
        ExecutionConfig.GlobalJobParameters globalParams = streamExecutionEnvironment.getConfig().getGlobalJobParameters();
        System.out.println("-------------------------------params_json=" + params_json + "----------globalParams =" + objectMapper.writeValueAsString(globalParams.toMap()));


        // create a table with example data without a connector required
        final Table rawCustomers =
                env.fromValues(
                        Row.of(
                                "Guillermo Smith",
                                LocalDate.parse("1992-12-12"),
                                "4081 Valley Road",
                                "08540",
                                "New Jersey",
                                "m",
                                true,
                                0,
                                78,
                                3),
                        Row.of(
                                "Valeria Mendoza",
                                LocalDate.parse("1970-03-28"),
                                "1239  Rainbow Road",
                                "90017",
                                "Los Angeles",
                                "f",
                                true,
                                9,
                                39,
                                0),
                        Row.of(
                                "Leann Holloway",
                                LocalDate.parse("1989-05-21"),
                                "2359 New Street",
                                "97401",
                                "Eugene",
                                null,
                                true,
                                null,
                                null,
                                null),
                        Row.of(
                                "Brandy Sanders",
                                LocalDate.parse("1956-05-26"),
                                "4891 Walkers-Ridge-Way",
                                "73119",
                                "Oklahoma City",
                                "m",
                                false,
                                9,
                                39,
                                0),
                        Row.of(
                                "John Turner",
                                LocalDate.parse("1982-10-02"),
                                "2359 New Street",
                                "60605",
                                "Chicago",
                                "m",
                                true,
                                12,
                                39,
                                0),
                        Row.of(
                                "Ellen Ortega",
                                LocalDate.parse("1985-06-18"),
                                "2448 Rodney STreet",
                                "85023",
                                "Phoenix",
                                "f",
                                true,
                                0,
                                78,
                                3));

        // handle ranges of columns easily
        final Table truncatedCustomers = rawCustomers.select(withColumns(range(1, 7)));

        // name columns
        final Table namedCustomers =
                truncatedCustomers.as(
                        "name",
                        "date_of_birth",
                        "street",
                        "zip_code",
                        "city",
                        "gender",
                        "has_newsletter");

        // register a view temporarily
        env.createTemporaryView("customers", namedCustomers);

        // use SQL whenever you like
        // call execute() and print() to get insights
        env.sqlQuery(
                        "SELECT "
                                + "  COUNT(*) AS `number of customers`, "
                                + "  AVG(YEAR(date_of_birth)) AS `average birth year` "
                                + "FROM `customers`")
                .execute()
                .print();

        // or further transform the data using the fluent Table API
        // e.g. filter, project fields, or call a user-defined function
        final Table youngCustomers =
                env.from("customers")
                        .filter($("gender").isNotNull())
                        .filter($("has_newsletter").isEqual(true))
                        .filter($("date_of_birth").isGreaterOrEqual(LocalDate.parse("1980-01-01")))
                        .select(
                                $("name").upperCase(),
                                $("date_of_birth"),
                                call(AddressNormalizer.class, $("street"), $("zip_code"), $("city"))
                                        .as("address"));

        // use execute() and collect() to retrieve your results from the cluster
        // this can be useful for testing before storing it in an external system
        try (CloseableIterator<Row> iterator = youngCustomers.execute().collect()) {
            final Set<Row> expectedOutput = new HashSet<>();
            expectedOutput.add(
                    Row.of(
                            "GUILLERMO SMITH",
                            LocalDate.parse("1992-12-12"),
                            "4081 VALLEY ROAD, 08540, NEW JERSEY"));
            expectedOutput.add(
                    Row.of(
                            "JOHN TURNER",
                            LocalDate.parse("1982-10-02"),
                            "2359 NEW STREET, 60605, CHICAGO"));
            expectedOutput.add(
                    Row.of(
                            "ELLEN ORTEGA",
                            LocalDate.parse("1985-06-18"),
                            "2448 RODNEY STREET, 85023, PHOENIX"));

            final Set<Row> actualOutput = new HashSet<>();
            iterator.forEachRemaining(actualOutput::add);

            if (actualOutput.equals(expectedOutput)) {
                System.out.println("SUCCESS!");
            } else {
                System.out.println("FAILURE!");
            }
        }
    }

    /**
     * We can put frequently used procedures in user-defined functions.
     *
     * <p>It is possible to call third-party libraries here as well.
     */
    public static class AddressNormalizer extends ScalarFunction {

        // the 'eval()' method defines input and output types (reflectively extracted)
        // and contains the runtime logic
        public String eval(String street, String zipCode, String city) {
            return normalize(street) + ", " + normalize(zipCode) + ", " + normalize(city);
        }

        private String normalize(String s) {
            return s.toUpperCase().replaceAll("\\W", " ").replaceAll("\\s+", " ").trim();
        }
    }
}
