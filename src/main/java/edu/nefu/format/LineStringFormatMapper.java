
package edu.nefu.format;


import edu.nefu.emus.FileDataSplitter;
import edu.nefu.emus.GeometryType;

public class LineStringFormatMapper
        extends FormatMapper
{

    /**
     * Instantiates a new line string format mapper.
     *
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public LineStringFormatMapper(FileDataSplitter Splitter, boolean carryInputData)
    {
        super(0, -1, Splitter, carryInputData, GeometryType.LINESTRING);
    }

    /**
     * Instantiates a new line string format mapper.
     *
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public LineStringFormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter,
            boolean carryInputData)
    {
        super(startOffset, endOffset, Splitter, carryInputData, GeometryType.LINESTRING);
    }
}