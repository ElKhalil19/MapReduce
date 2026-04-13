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