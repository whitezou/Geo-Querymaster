package edu.nefu.gqtree;

public class GQPoint {
    double x, y;
    int IntX, IntY;

    public GQPoint() {
        this.x = 0;
        this.y = 0;
    }

    public GQPoint(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public GQPoint(int x, int y) {
        this.IntX = x;
        this.IntY = y;
    }

    public double caculateDistance(GQPoint p) {
        return Math.sqrt(
                Math.pow((this.IntX - p.IntX), 2) + Math.pow((this.IntY - p.IntY), 2)
        );
    }

}
