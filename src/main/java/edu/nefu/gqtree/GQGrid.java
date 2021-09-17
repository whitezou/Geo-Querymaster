package edu.nefu.gqtree;

import java.io.Serializable;

public class GQGrid<T> implements Serializable {
    //    ArrayList<ArrayList<GQuadNode>> cell;
    GQuadNode<T>[][] cell;

    public GQGrid(int cell_number_x, int cell_number_y, GQuadNode root) {
        cell = new GQuadNode[cell_number_x][cell_number_y];
        for (int i = 0; i < cell_number_x; i++) {
            for (int j = 0; j < cell_number_y; j++) {
                cell[i][j] = root;
            }
        }
    }

}
