
package edu.nefu.gqtree;


import java.io.Serializable;

public class GQuadNode<T> implements Serializable {
    GQuadRectangle r;
    T element;

    GQuadNode sw = null;
    GQuadNode se = null;
    GQuadNode nw = null;
    GQuadNode ne = null;

    GQuadNode(GQuadRectangle r, T element) {
        this.r = r;
        this.element = element;
    }

    @Override
    public String toString() {
        return r.toString();
    }
}
