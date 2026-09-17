# MapReduce Employee Contributions — Project Report

## 1. Project Setup (Maven Configuration)

### `pom.xml`

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">

  <modelVersion>4.0.0</modelVersion>

  <groupId>com.hadoop.mapreduce</groupId>
  <artifactId>EmployeeContributions</artifactId>
  <version>1.0-SNAPSHOT</version>
  <packaging>jar</packaging>

  <dependencies>
    <!-- Hadoop Common Library -->
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-common</artifactId>
      <version>3.3.0</version>
    </dependency>

    <!-- Hadoop MapReduce Client -->
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-mapreduce-client-core</artifactId>
      <version>3.3.0</version>
    </dependency>

    <!-- Hadoop HDFS -->
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-hdfs</artifactId>
      <version>3.3.0</version>
    </dependency>

    <!-- Logging Dependencies -->
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-api</artifactId>
      <version>1.7.30</version>
    </dependency>

    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-log4j12</artifactId>
      <version>1.7.30</version>
    </dependency>

    <!-- JUnit for Testing -->
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>4.12</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.8.1</version>
        <configuration>
          <source>21</source>
          <target>21</target>
        </configuration>
      </plugin>
    </plugins>
  </build>

</project>
```

### Project Metadata

| Field        | Value                    |
|--------------|--------------------------|
| GroupId      | `com.hadoop.mapreduce`   |
| ArtifactId   | `EmployeeContributions`  |
| Version      | `1.0-SNAPSHOT`           |
| Packaging    | `jar`                    |

### Dependencies

| Dependency                                | Version  | Scope   |
|-------------------------------------------|----------|---------|
| `org.apache.hadoop:hadoop-common`         | `3.3.0`  | compile |
| `org.apache.hadoop:hadoop-mapreduce-client-core` | `3.3.0` | compile |
| `org.apache.hadoop:hadoop-hdfs`           | `3.3.0`  | compile |
| `org.slf4j:slf4j-api`                     | `1.7.30` | compile |
| `org.slf4j:slf4j-log4j12`                 | `1.7.30` | compile |
| `junit:junit`                             | `4.12`   | test    |

### Build Configuration

The Maven Compiler Plugin (`3.8.1`) is configured to target **Java 21** for both the source and target compatibility levels.

---

## 2. Mapper Class Implementation (`ContributionsMapper.java`)

### Full Source

```java
package com.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ContributionsMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final Text employeeId = new Text();
    private final IntWritable contributionAmount = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split(",");

        // Skip header line
        if (!fields[0].equals("year")) {
            try {
                String id = fields[2].trim();                 // Employee ID
                int amount = Integer.parseInt(fields[3].trim()); // Contribution Amount

                employeeId.set(id);
                contributionAmount.set(amount);

                context.write(employeeId, contributionAmount);
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid line: " + line);
            }
        }
    }
}
```

### Description

| Item                | Detail                                                                 |
|---------------------|------------------------------------------------------------------------|
| **Class**           | `ContributionsMapper`                                                  |
| **Extends**         | `Mapper<Object, Text, Text, IntWritable>`                              |
| **Input key type**  | `Object` (byte offset within the input file)                           |
| **Input value type**| `Text` (one CSV line)                                                  |
| **Output key type** | `Text` (employee ID)                                                   |
| **Output value type**| `IntWritable` (contribution amount)                                   |

### Logic

1. Each input line is split on commas to extract individual fields.
2. The header row (where `fields[0]` equals `"year"`) is skipped.
3. For every data row:
   - `fields[2]` is taken as the **Employee ID** (trimmed).
   - `fields[3]` is parsed as an **integer contribution amount** (trimmed).
4. The `(employeeId, contributionAmount)` pair is emitted to the context.
5. Lines that cannot be parsed (e.g., non-numeric amounts) are logged to `stderr` and skipped gracefully.

---

## 3. Reducer Class Implementation (`ContributionsReducer.java`)

### Full Source

```java
package com.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ContributionsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }

        outValue.set(sum);
        context.write(key, outValue);
    }
}
```

### Description

| Item                 | Detail                                                              |
|----------------------|---------------------------------------------------------------------|
| **Class**            | `ContributionsReducer`                                              |
| **Extends**          | `Reducer<Text, IntWritable, Text, IntWritable>`                     |
| **Input key type**   | `Text` (employee ID)                                                |
| **Input value type** | `IntWritable` (individual contribution amounts from the Mapper)     |
| **Output key type**  | `Text` (employee ID)                                                |
| **Output value type**| `IntWritable` (total contributions for the employee)                |

### Logic

1. The reducer receives all contribution amounts grouped by employee ID.
2. It iterates over the values and accumulates a running sum.
3. The total sum is written to the context as `(employeeId, totalContributions)`.

---

## 4. Job Driver Class Implementation (`ContributionsJob.java`)

### Full Source

```java
package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ContributionsJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ContributionsJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Employee Contributions");

        job.setJarByClass(ContributionsJob.class);

        job.setMapperClass(ContributionsMapper.class);
        job.setReducerClass(ContributionsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

### Description

| Item              | Detail                                              |
|-------------------|-----------------------------------------------------|
| **Class**         | `ContributionsJob`                                  |
| **Entry point**   | `main(String[] args)`                               |
| **Arguments**     | `args[0]` — HDFS input path; `args[1]` — HDFS output path |
| **Job name**      | `"Employee Contributions"`                          |
| **Mapper class**  | `ContributionsMapper`                               |
| **Reducer class** | `ContributionsReducer`                              |
| **Output key**    | `Text` (employee ID)                                |
| **Output value**  | `IntWritable` (total contributions)                 |

### Logic

1. Validates that exactly two command-line arguments are provided (input and output paths).
2. Creates a Hadoop `Configuration` and a `Job` instance named `"Employee Contributions"`.
3. Registers the Mapper, Reducer, and output types.
4. Sets the input and output HDFS paths from the command-line arguments.
5. Submits the job and waits for completion, exiting with status `0` on success or `1` on failure.

---

## 5. How to Build and Run

### Build

```bash
mvn clean package
```

This produces `target/EmployeeContributions-1.0-SNAPSHOT.jar`.

### Run on Hadoop

```bash
hadoop jar target/EmployeeContributions-1.0-SNAPSHOT.jar \
    com.hadoop.mapreduce.ContributionsJob \
    /input/contributions.csv \
    /output/contributions_result
```

### Expected Input Format

The input CSV file must have (at minimum) four columns:

```
year,<col2>,employee_id,contribution_amount,...
```

Example:

```
year,department,emp_id,amount
2023,Engineering,E001,5000
2023,Marketing,E002,3000
2024,Engineering,E001,7000
```

### Expected Output Format

Each output line contains an employee ID and their **total** contribution amount:

```
E001    12000
E002    3000
```
