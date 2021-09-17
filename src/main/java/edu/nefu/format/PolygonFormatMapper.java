
package edu.nefu.format;

import edu.nefu.emus.FileDataSplitter;
import edu.nefu.emus.GeometryType;

public class PolygonFormatMapper
        extends FormatMapper
{

    /**
     * Instantiates a new polygon format mapper.
     *
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public PolygonFormatMapper(FileDataSplitter Splitter, boolean carryInputData)
    {
        super(0, -1, Splitter, carryInputData, GeometryType.POLYGON);
    }

    /**
     * Instantiates a new polygon format mapper.
     *
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public PolygonFormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter,
            boolean carryInputData)
    {
        super(startOffset, endOffset, Splitter, carryInputData, GeometryType.POLYGON);
    }
}