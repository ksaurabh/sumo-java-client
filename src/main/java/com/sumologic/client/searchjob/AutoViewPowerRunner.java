package com.sumologic.client.searchjob;

import com.sumologic.client.SumoException;
import com.sumologic.client.SumoLogicClient;
import com.sumologic.client.model.LogMessage;
import com.sumologic.client.searchjob.model.GetMessagesForSearchJobRequest;
import com.sumologic.client.searchjob.model.GetMessagesForSearchJobResponse;
import com.sumologic.client.searchjob.model.GetRecordsForSearchJobResponse;
import com.sumologic.client.searchjob.model.SearchJobRecord;

import java.io.*;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Scanner;
import java.util.TimeZone;

public class AutoViewPowerRunner {

    public static void main(String[] args) throws ParseException, InterruptedException, FileNotFoundException {
        Scanner scanner;
        if(args.length == 1){
            String paramsFile = args[0];
            System.out.println("Reading run parameters from " + paramsFile);
            scanner = new Scanner(new FileInputStream(paramsFile));
        } else {
            scanner = new Scanner(System.in);
        }

        scanner.useDelimiter(System.getProperty("line.separator"));

        System.out.print("Enter user name: ");
        String username = scanner.next();

        System.out.print("Enter password: ");
        String password = scanner.next().trim();

        System.out.print("Enter service url (something like https://service.sumologic.com): ");
        String url = scanner.next();

        System.out.println("Connecting to " + url + " with username " + username + " and password: " + password);



        System.out.print("Enter location of queryfile: ");
        String filename = scanner.next();
        String query = null;
        try {
            query = readFile(filename);
            System.out.println("Reading query from file " + filename + ", found the following:");
            System.out.println(query);
        } catch(Exception e){
            System.err.println(e);
            System.exit(1);
        }

        System.out.print("Is this an aggregate query? (true/false): ");
        boolean isAggQuery = scanner.nextBoolean();

        String dateFormatString = "MM/dd/yy HH:mm";
        System.out.println();
        System.out.println("Date format we expect is: " + dateFormatString);
        System.out.println();


        System.out.print("Enter starttime: ");
        String st = scanner.next();
        String startTimeString = st.trim();

        System.out.print("Enter endTime: ");
        String endTimeString = scanner.next().trim();

        System.out.println("Time range read is " +  startTimeString + " to " + endTimeString);

        SimpleDateFormat dateFormat =  new SimpleDateFormat(dateFormatString);
        long startTime = dateFormat.parse(startTimeString).getTime();
        long endTime = dateFormat.parse(endTimeString).getTime();

        System.out.println("Enter batchsize in minutes: ");
        long batchSizeInMinutes = scanner.nextLong();
        SimpleDateFormat timeRangeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");


        SumoLogicClient client = new SumoLogicClient(username, password);
        try {
            client.setURL(url);
        } catch (MalformedURLException e) {
            System.err.println(e);
            System.exit(1);
        }

        try {
            // Search for log lines containing "error"
            String timeZone = TimeZone.getDefault().getID();
            System.out.println("Timezone used = " + timeZone);

            long currentEndTime = endTime;
            long currentStartTime = Math.max(endTime - batchSizeInMinutes * 60 * 1000, startTime);
            while(currentStartTime < currentEndTime){
                String cst = timeRangeFormat.format(new Date(currentStartTime));
                String cet = timeRangeFormat.format(new Date(currentEndTime));

                System.out.println("Current time range: " + cst + " to " + cet);
                System.out.println("sleeping for 5 seconds before next run..");
                Thread.sleep(5000);

                String jobid = client.createSearchJob(query, cst, cet, timeZone);
                long runStartTime = System.currentTimeMillis();
                String state = client.getSearchJobStatus(jobid).getState();
                for(int i =0; i < 1200; i++){
                    state = client.getSearchJobStatus(jobid).getState();
                    System.out.println("Current state = " + state);
                    if(state.trim().equals( "DONE GATHERING RESULTS")){
                        System.out.println("Here is the output: ");
                        if (isAggQuery) {
                            GetRecordsForSearchJobResponse results = client.getRecordsForSearchJob(jobid, 0, 10);
                            for(SearchJobRecord r: results.getRecords()){
                                Map<String, String> m = r.getMap();
                                for(String k: m.keySet()){
                                    System.out.print(k + " -> " + m.get(k) + "\t");
                                }
                                System.out.println();
                            }
                        } else {
                            GetMessagesForSearchJobResponse results = client.getMessagesForSearchJob(jobid, 0, 10);
                            for(LogMessage r: results.getMessages()){
                                System.out.println(r);
                            }
                        }
                        
                        long timetaken = System.currentTimeMillis() - runStartTime;
                        int timeTakenInMinutes = (int) (timetaken/60000);
                        System.out.println("Time taken for last run = " + timeTakenInMinutes + " minutes");

                        break;
                    }
                    Thread.sleep(1000);
                }

                if(!state.trim().equals( "DONE GATHERING RESULTS")){
                    System.err.println("Last run timed out...");
                }

                // update the time range.
                currentEndTime = currentStartTime;
                currentStartTime = Math.max(currentEndTime - batchSizeInMinutes * 60 * 1000, startTime);
            }
        } catch (SumoException e) {
            e.printStackTrace();
        }
    }

    private static String readFile( String file ) throws IOException {
        BufferedReader reader = new BufferedReader( new FileReader(file));
        String         line = null;
        StringBuilder  stringBuilder = new StringBuilder();
        String         ls = System.getProperty("line.separator");

        while( ( line = reader.readLine() ) != null ) {
            stringBuilder.append( line );
            stringBuilder.append( ls );
        }

        return stringBuilder.toString();
    }
}
