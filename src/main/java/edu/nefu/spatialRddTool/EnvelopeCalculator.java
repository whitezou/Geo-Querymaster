package edu.nefu.spatialRddTool;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.Objects;

public class EnvelopeCalculator
        implements Serializable
{
    private Envelope boundary;
    public EnvelopeCalculator(){
        this.boundary = null;
    }
    public EnvelopeCalculator(Envelope boundary)
    {
        Objects.requireNonNull(boundary, "Boundary cannot be null");

        this.boundary = boundary;
    }

    public static EnvelopeCalculator combine(EnvelopeCalculator agg1, EnvelopeCalculator agg2)
            throws Exception
    {
        if (agg1 == null) {
            return agg2;
        }

        if (agg2 == null) {
            return agg1;
        }

        return new EnvelopeCalculator(
                EnvelopeCalculator.combine(agg1.boundary, agg2.boundary));
    }

    public static Envelope combine(Envelope agg1, Envelope agg2)
            throws Exception
    {
        if (agg1 == null) {
            return agg2;
        }

        if (agg2 == null) {
            return agg1;
        }

        return new Envelope(
                Math.min(agg1.getMinX(), agg2.getMinX()),
                Math.max(agg1.getMaxX(), agg2.getMaxX()),
                Math.min(agg1.getMinY(), agg2.getMinY()),
                Math.max(agg1.getMaxY(), agg2.getMaxY()));
    }

    public static Envelope add(Envelope agg, Geometry object)
            throws Exception
    {
        return combine(object.getEnvelopeInternal(), agg);
    }

    public static EnvelopeCalculator add(EnvelopeCalculator agg, Geometry object)
            throws Exception
    {
        return combine(new EnvelopeCalculator(object.getEnvelopeInternal()), agg);
    }

    public Envelope getBoundary()
    {
        return boundary;
    }


}
