package calculator;

/**
 * Created by sank on 10/14/16.
 */
public class Driver {
    public static void main(String[] args) throws Exception {
        /**
         * variables for usage throughout the program
         */
        String input = args[0];
        String output = args[1];
        String factor = args[2];
        String graphOutput = args[1] + "graph/";
        String alteredInput = args[1] + "alteredInput/";
        String temp = args[1] + "temp/";
        String finalOutput = args[1] + "output";
        String topPages = args[1] + "top10Nodes";


        int count = 0;
        int prev = 0;
        long nonConvergentNodes = 0;
        long timeForIterations = 0;
        /**
         * storing the start time of the program
         */
        long startTime = System.currentTimeMillis();
        long endTime = 0;

        /**
         * instantiating all the required classes
         */
        GraphPropCal graph = new GraphPropCal();
        AlterInput alterInput = new AlterInput();
        AlterOutput alterOutput = new AlterOutput();
        RankCal rankCalculator = new RankCal();
        TopPagesCal topPagesCal = new TopPagesCal();

        /**
         * get graph properties job
         */
        graph.getGraph(input, graphOutput);
        /**
         * change the input format for running pagerank, stores it in temp location
         */
        alterInput.alter(input, alteredInput);


        /**
         * repeats the iterations till non-converganet node count is 0
         */
        while (true) {
            count++;
            if (count == 1) {
                nonConvergentNodes = rankCalculator.calculatePageRank(alteredInput, temp + count, factor);
                prev = count;
            } else {
                nonConvergentNodes = rankCalculator.calculatePageRank(temp + prev, temp + count, factor);
                prev = count;
            }
            if (nonConvergentNodes == 0) {
                break;
            }
            if (count == 10) {
                /**
                 * storing time for 10 iterations
                 */
                timeForIterations = System.currentTimeMillis();
            }
        }

        /**
         * modifying out put to store page and rank in desired format
         */
        alterOutput.print(temp + count, finalOutput);
        /**
         * calculates the top 10 pages from the page and rank output from previous job.
         */
        topPagesCal.topPages(finalOutput, topPages);
        /**
         * storing the end time
         */
        endTime = System.currentTimeMillis();

        /**
         * printing all the graph details
         */
        System.out.println("Graph details : " + graphOutput);
        System.out.println("Page rank output location : " + finalOutput);
        System.out.println("List of Top 10 nodes are calculated and stored in " + args[1] + "top10Nodes");
        System.out.println("Total iterations :  " + count);
        System.out.println("Time for 10 iterations : " + (timeForIterations - startTime) + "ms");
        System.out.println("Total execution time : " + (endTime - startTime) + "ms");
    }
}
