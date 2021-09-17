package edu.nefu.data_utils;

public class ParamPaser {
    public void setBbox(double[] bbox) {
        this.bbox = bbox;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public double[] getBbox() {
        return bbox;
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    double[] bbox;
    int width;
    int height;
}
