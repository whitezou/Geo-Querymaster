package edu.nefu.emus;


import java.io.Serializable;

// TODO: Auto-generated Javadoc

/**
 * The Enum GridType.
 */
public enum GridType
        implements Serializable
{

    /**
     * The equalgrid.
     */
    EQUALGRID,

    /**
     * The quadtree.
     */
    QUADTREE,

    /**
     * The buffer tree.
     */

    BUFFEREDTREE;

    /**
     * Gets the grid type.
     *
     * @param str the str
     * @return the grid type
     */

    public static GridType getGridType(String str)
    {
        for (GridType me : GridType.values()) {
            if (me.name().equalsIgnoreCase(str)) { return me; }
        }
        return null;
    }
}
