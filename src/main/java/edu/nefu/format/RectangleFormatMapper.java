
package edu.nefu.format;

import edu.nefu.emus.FileDataSplitter;
import edu.nefu.emus.GeometryType;

public class RectangleFormatMapper
        extends FormatMapper
{

    /**
     * Instantiates a new rectangle format mapper.
     *
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public RectangleFormatMapper(FileDataSplitter Splitter, boolean carryInputData)
    {
        super(0, 3, Splitter, carryInputData, GeometryType.RECTANGLE);
    }

    /**
     * Instantiates a new rectangle format mapper.
     *
     * @param startOffset the start offset
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public RectangleFormatMapper(Integer startOffset, FileDataSplitter Splitter,
            boolean carryInputData)
    {
        super(startOffset, startOffset+3, Splitter, carryInputData, GeometryType.RECTANGLE);
    }
}
