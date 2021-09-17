package org.apache.spark.rdd;

public class IntegerData {


    private int partitionID;
    private int start;
    private int end;
    private  int currentIndex;


    public IntegerData(int partitionID, int start, int end) {
        this.partitionID = partitionID;
        this.start = start;
        this.end = end;
        currentIndex = start;
    }
    public boolean hasnext() {
        return currentIndex<=end;
    }
    public int next() throws Exception {
        if (currentIndex <= end) {
            return currentIndex++;
        }
        throw new Exception();

    }

    @Override
    public String toString() {
        return "IntegerData{" +
                "partitionID=" + partitionID +
                ", start=" + start +
                ", end=" + end +
                ", currentIndex=" + currentIndex +
                '}';
    }
}
