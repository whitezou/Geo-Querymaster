package edu.nefu.DebugQTC.partition;

import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO: Auto-generated Javadoc

/**
 * The Class EqualPartitioning.
 */
public class EqualPartitioning
        implements Serializable {

    /**
     * The grids.
     */
    List<Envelope> grids = new ArrayList<Envelope>();

    Map partitionGrids = new HashMap<Integer, Envelope>();

    /**
     * Instantiates a new equal partitioning.
     *
     * @param boundary   the boundary
     * @param partitions the partitions
     */
    public EqualPartitioning(Envelope boundary, int partitions) {
        //Local variable should be declared here
        Double root = Math.sqrt(partitions);
        int partitionsAxis;
        double intervalX;
        double intervalY;

        //Calculate how many bounds should be on each axis
        partitionsAxis = root.intValue();
        intervalX = (boundary.getMaxX() - boundary.getMinX()) / partitionsAxis;
        intervalY = (boundary.getMaxY() - boundary.getMinY()) / partitionsAxis;
        //System.out.println("Boundary: "+boundary+"root: "+root+" interval: "+intervalX+","+intervalY);
        for (int i = 0; i < partitionsAxis; i++) {
            for (int j = 0; j < partitionsAxis; j++) {
                Envelope grid = new Envelope(boundary.getMinX() + intervalX * i, boundary.getMinX() + intervalX * (i + 1), boundary.getMinY() + intervalY * j, boundary.getMinY() + intervalY * (j + 1));
                //System.out.println("Grid: "+grid);
                grids.add(grid);
                partitionGrids.put(i * partitionsAxis + j, grid);
            }
            //System.out.println("Finish one column/one certain x");
        }
    }

    /**
     * Gets the grids.
     *
     * @return the grids
     */
    public List<Envelope> getGrids() {

        return this.grids;
    }

    public Map<Integer, Envelope> getPartitionGrids() {

        return this.partitionGrids;
    }
}
