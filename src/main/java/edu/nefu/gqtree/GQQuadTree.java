package edu.nefu.gqtree;

import org.locationtech.jts.geom.Envelope;
import org.apache.commons.lang3.mutable.MutableInt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class GQQuadTree<T> implements Serializable {
    // Maximum number of items in any given zone. When reached, a zone is sub-divided.
    private final int maxItemsPerZone;
    private final int maxLevel;

    private final int level;
    private int nodeNum = 0;

    // the four sub regions,
    // may be null if not needed
    //region是当前节点的子树
    private GQQuadTree<T>[] regions;

    // the current nodes
    //存储当前节点的四个子节点的矩形框
    private final List<GQuadNode<T>> nodes = new ArrayList<>();

    // current rectangle zone
    private final GQuadRectangle zone;

    public static final int REGION_SELF = -1;
    public static final int REGION_NW = 0;
    public static final int REGION_NE = 1;
    public static final int REGION_SW = 2;
    public static final int REGION_SE = 3;

    public GQQuadTree(GQuadRectangle definition, int level) {
        this(definition, level, 5, 10);
    }

    public GQQuadTree(GQuadRectangle definition, int level, int maxItemsPerZone, int maxLevel) {
        this.maxItemsPerZone = maxItemsPerZone;
        this.maxLevel = maxLevel;
        this.zone = definition;
        this.level = level;
    }

    public GQuadRectangle getZone() {
        return this.zone;
    }

    private int findRegion(GQuadRectangle r, boolean split) {
        int region = REGION_SELF;
        if (nodeNum >= maxItemsPerZone && this.level < maxLevel) {
            // we don't want to split if we just need to retrieve
            // the region, not inserting an element
            if (regions == null && split) {
                // then create the subregions
                this.split();
            }

            // can be null if not splitted
            if (regions != null) {
                for (int i = 0; i < regions.length; i++) {
                    if (regions[i].getZone().contains(r)) {
                        region = i;
                        break;
                    }
                }
            }
        }
        return region;
    }

    private int findRegion(int x, int y) {
        int region = REGION_SELF;
        // can be null if not splitted
        if (regions != null) {
            for (int i = 0; i < regions.length; i++) {
                if (regions[i].getZone().contains(x, y)) {
                    region = i;
                    break;
                }
            }
        }
        return region;
    }

    private GQQuadTree<T> newQuadTree(GQuadRectangle zone, int level) {
        return new GQQuadTree<T>(zone, level, this.maxItemsPerZone, this.maxLevel);
    }

    private void split() {

        regions = new GQQuadTree[4];

        double newWidth = zone.width / 2;
        double newHeight = zone.height / 2;
        int newLevel = level + 1;

        regions[REGION_NW] = newQuadTree(new GQuadRectangle(
                zone.x,
                zone.y + zone.height / 2,
                newWidth,
                newHeight
        ), newLevel);

        regions[REGION_NE] = newQuadTree(new GQuadRectangle(
                zone.x + zone.width / 2,
                zone.y + zone.height / 2,
                newWidth,
                newHeight
        ), newLevel);

        regions[REGION_SW] = newQuadTree(new GQuadRectangle(
                zone.x,
                zone.y,
                newWidth,
                newHeight
        ), newLevel);

        regions[REGION_SE] = newQuadTree(new GQuadRectangle(
                zone.x + zone.width / 2,
                zone.y,
                newWidth,
                newHeight
        ), newLevel);
    }

    // Force the quad tree to grow up to a certain level.
    public void forceGrowUp(int minLevel) {
        if (minLevel < 1) {
            throw new IllegalArgumentException("minLevel must be >= 1. Received " + minLevel);
        }

        split();
        nodeNum = maxItemsPerZone;
        if (level + 1 >= minLevel) {

            return;
        }

        for (GQQuadTree<T> region : regions) {
            region.forceGrowUp(minLevel);
        }
    }

    public void insert(GQuadRectangle r, T element) {
        int region = this.findRegion(r, true);
        if (region == REGION_SELF || this.level == maxLevel) {
            nodes.add(new GQuadNode<T>(r, element));
            nodeNum++;
            return;
        } else {
            regions[region].insert(r, element);
        }

        if (nodeNum >= maxItemsPerZone && this.level < maxLevel) {
            // redispatch the elements
            List<GQuadNode<T>> tempNodes = new ArrayList<>();
            tempNodes.addAll(nodes);

            nodes.clear();
            for (GQuadNode<T> node : tempNodes) {
                this.insert(node.r, node.element);
            }
        }
    }

    public void dropElements() {
        traverse(new GQQuadTree.Visitor<T>() {
            @Override
            public boolean visit(GQQuadTree<T> tree) {
                tree.nodes.clear();
                return true;
            }
        });
    }

    public List<T> getElements(GQuadRectangle r) {
        int region = this.findRegion(r, false);

        final List<T> list = new ArrayList<>();

        if (region != REGION_SELF) {
            for (GQuadNode<T> node : nodes) {
                list.add(node.element);
            }

            list.addAll(regions[region].getElements(r));
        } else {
            addAllElements(list);
        }

        return list;
    }

    private interface Visitor<T> {
        /**
         * Visits a single node of the tree
         *
         * @param tree Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(GQQuadTree<T> tree);
    }

    private interface VisitorWithLineage<T> {
        /**
         * Visits a single node of the tree, with the traversal trace
         *
         * @param tree Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(GQQuadTree<T> tree, String lineage);
    }

    /**
     * Traverses the tree top-down breadth-first and calls the visitor
     * for each node. Stops traversing if a call to Visitor.visit returns false.
     */
    private void traverse(GQQuadTree.Visitor<T> visitor) {
        if (!visitor.visit(this)) {
            return;
        }

        if (regions != null) {
            regions[REGION_NW].traverse(visitor);
            regions[REGION_NE].traverse(visitor);
            regions[REGION_SW].traverse(visitor);
            regions[REGION_SE].traverse(visitor);
        }
    }

    /**
     * Traverses the tree top-down breadth-first and calls the visitor
     * for each node. Stops traversing if a call to Visitor.visit returns false.
     * lineage will memorize the traversal path for each nodes
     */
    private void traverseWithTrace(GQQuadTree.VisitorWithLineage<T> visitor, String lineage) {
        if (!visitor.visit(this, lineage)) {
            return;
        }

        if (regions != null) {
            regions[REGION_NW].traverseWithTrace(visitor, lineage + REGION_NW);
            regions[REGION_NE].traverseWithTrace(visitor, lineage + REGION_NE);
            regions[REGION_SW].traverseWithTrace(visitor, lineage + REGION_SW);
            regions[REGION_SE].traverseWithTrace(visitor, lineage + REGION_SE);
        }
    }

    private void addAllElements(final List<T> list) {
        traverse(new GQQuadTree.Visitor<T>() {
            @Override
            public boolean visit(GQQuadTree<T> tree) {
                for (GQuadNode<T> node : tree.nodes) {
                    list.add(node.element);
                }
                return true;
            }
        });
    }

    public boolean isLeaf() {
        return regions == null;
    }

    public List<GQuadRectangle> getAllZones() {
        final List<GQuadRectangle> zones = new ArrayList<>();
        traverse(new GQQuadTree.Visitor<T>() {
            @Override
            public boolean visit(GQQuadTree<T> tree) {
                zones.add(tree.zone);
                return true;
            }
        });

        return zones;
    }

    public List<GQuadRectangle> getLeafZones() {
        final List<GQuadRectangle> leafZones = new ArrayList<>();
        traverse(new GQQuadTree.Visitor<T>() {
            @Override
            public boolean visit(GQQuadTree<T> tree) {
                if (tree.isLeaf()) {
                    leafZones.add(tree.zone);
                }
                return true;
            }
        });

        return leafZones;
    }

    public int getTotalNumLeafNode() {
        final MutableInt leafCount = new MutableInt(0);
        traverse(new GQQuadTree.Visitor<T>() {
            @Override
            public boolean visit(GQQuadTree<T> tree) {
                if (tree.isLeaf()) {
                    leafCount.increment();
                }
                return true;
            }
        });

        return leafCount.getValue();
    }

    /**
     * Find the zone that fully contains this query point
     *
     * @param x
     * @param y
     * @return
     */
    public GQuadRectangle getZone(int x, int y)
            throws ArrayIndexOutOfBoundsException {
        int region = this.findRegion(x, y);
        if (region != REGION_SELF) {
            return regions[region].getZone(x, y);
        } else {
            if (this.zone.contains(x, y)) {
                return this.zone;
            }

            throw new ArrayIndexOutOfBoundsException("this pixel is out of the quad tree boundary.");
        }
    }

    public GQuadRectangle getParentZone(int x, int y, int minLevel)
            throws Exception {
        int region = this.findRegion(x, y);
        // Assume this quad tree has done force grow up. Thus, the min tree depth is the min tree level
        if (level < minLevel) {
            // In our case, this node must have child nodes. But, in general, if the region is still -1, that means none of its child contains
            // the given x and y
            if (region == REGION_SELF) {
                assert regions == null;
                if (zone.contains(x, y)) {
                    // This should not happen
                    throw new Exception("[GridQuadTree][getParentZone] this leaf node doesn't have enough depth. " +
                            "Please check ForceGrowUp. Expected: " + minLevel + " Actual: " + level + ". Query point: " + x + " " + y +
                            ". Tree statistics, total leaf nodes: " + getTotalNumLeafNode());
                } else {
                    throw new Exception("[GridQuadTree][getParentZone] this pixel is out of the quad tree boundary.");
                }
            } else {
                return regions[region].getParentZone(x, y, minLevel);
            }
        }
        if (zone.contains(x, y)) {
            return zone;
        }

        throw new Exception("[getParentZone] this pixel is out of the quad tree boundary.");
    }

    public List<GQuadRectangle> findZones(GQuadRectangle r) {
        final Envelope envelope = r.getEnvelope();

        final List<GQuadRectangle> matches = new ArrayList<>();
        traverse(new GQQuadTree.Visitor<T>() {
            @Override
            public boolean visit(GQQuadTree<T> tree) {
                if (!disjoint(tree.zone.getEnvelope(), envelope)) {
                    if (tree.isLeaf()) {
                        matches.add(tree.zone);
                    }
                    return true;
                } else {
                    return false;
                }
            }
        });

        return matches;
    }

    private boolean disjoint(Envelope r1, Envelope r2) {
        return !r1.intersects(r2) && !r1.covers(r2) && !r2.covers(r1);
    }

    public void assignPartitionIds() {
        traverse(new GQQuadTree.Visitor<T>() {
            private int partitionId = 0;

            @Override
            public boolean visit(GQQuadTree<T> tree) {
                if (tree.isLeaf()) {
                    tree.getZone().partitionId = partitionId;
                    partitionId++;
                }
                return true;
            }
        });
    }

    public void assignPartitionLineage() {
        traverseWithTrace(new GQQuadTree.VisitorWithLineage<T>() {
            @Override
            public boolean visit(GQQuadTree<T> tree, String lineage) {
                if (tree.isLeaf()) {
                    tree.getZone().lineage = lineage;
                }
                return true;
            }
        }, "");
    }


}
