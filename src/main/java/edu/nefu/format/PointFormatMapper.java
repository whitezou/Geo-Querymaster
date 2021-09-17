
package edu.nefu.format;

import edu.nefu.emus.FileDataSplitter;
import edu.nefu.emus.GeometryType;

public class PointFormatMapper
        extends FormatMapper
{

    /**
     * Instantiates a new point format mapper.
     *
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public PointFormatMapper(FileDataSplitter Splitter, boolean carryInputData)
    {
        super(0, 1, Splitter, carryInputData, GeometryType.POINT);
    }

    /**
     * Instantiates a new point format mapper.
     *
     * @param startOffset the start offset
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public PointFormatMapper(Integer startOffset, FileDataSplitter Splitter,
            boolean carryInputData)
    {
        super(startOffset, startOffset+1, Splitter, carryInputData, GeometryType.POINT);
    }

}
