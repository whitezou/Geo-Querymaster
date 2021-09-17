package edu.nefu.data_utils

import java.awt.Color
import java.io.File
import java.util

import org.geotools.data.DataUtilities
import org.geotools.data.simple.SimpleFeatureSource
import org.locationtech.jts.geom.Envelope
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.map.{FeatureLayer, MapContent}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.styling.{SLD, Style}
import org.geotools.swing.JMapFrame
import org.locationtech.jts.geom.{GeometryFactory, LineString, MultiPolygon, Point}
import java.util
import java.util
//import scala.collection.immutable.HashMap

object Vis_range_Point extends App {
  val x1 = -50.3010141441
  val x2 = -24.9526465797
  val y1 = -53.209588996
  val y2 = -30.1096863746
  val rangeQueryWindow1 = new Envelope(-50.3010141441, -24.9526465797, -53.209588996, -30.1096863746)
  val rangeQueryWindow2 = new Envelope(-54.4270741441, -24.9526465797, -53.209588996, -30.1096863746)
  val rangeQueryWindow3 = new Envelope(-114.4270741441, 42.9526465797, -54.509588996, -27.0106863746)
  val rangeQueryWindow4 = new Envelope(-82.7638020000, 42.9526465797, -54.509588996, 38.0106863746)
  val rangeQueryWindow5 = new Envelope(-140.99778, 5.7305630159, -52.6480987209, 83.23324)
  val rangeQueryWindow6 = new Envelope(-180.0, 180.0, -90.0, 90.0)
  val geometryFactory = new GeometryFactory
  val envelope_Geometry1 = geometryFactory.toGeometry(rangeQueryWindow1)
  val envelope_Geometry2 = geometryFactory.toGeometry(rangeQueryWindow2)
  val envelope_Geometry3 = geometryFactory.toGeometry(rangeQueryWindow3)
  val envelope_Geometry4 = geometryFactory.toGeometry(rangeQueryWindow4)
  val envelope_Geometry5 = geometryFactory.toGeometry(rangeQueryWindow5)
  val envelope_Geometry6 = geometryFactory.toGeometry(rangeQueryWindow6)
  val geo_type = "linestring"
  val shpPath = ""
  println((x2 - x1) * (y2 - y1) / (360 * 180))

  val builder = new SimpleFeatureTypeBuilder
  builder.setName("TwoDistancesType")
  builder.setCRS(DefaultGeographicCRS.WGS84)
  //            builder.add("MultiPolygon", MultiPolygon.class);
  if ("multipolygon" == geo_type) builder.add("MultiPolygon", classOf[MultiPolygon])
  if ("multipolyline" == geo_type) builder.add("line", classOf[LineString])
  if ("linestring" == geo_type) builder.add("line", classOf[LineString])
  if ("point" == geo_type) builder.add("Point", classOf[Point])

  val styleColor1 = new Color(0, 0, 0)
  val styleColor2 = new Color(255, 0, 0)
  val styleColor3 = new Color(0, 255, 0)
  val styleColor4 = new Color(0, 0, 255)
  val styleColor5 = new Color(255, 255, 0)
  val styleColor6 = new Color(255, 255, 255)

  val TYPE = builder.buildFeatureType
  val featureBuilder = new SimpleFeatureBuilder(TYPE)
  val filePath = shpPath
  val encoding = "UTF-8"
  val file = new File(filePath)
  val featureCollection = new DefaultFeatureCollection("internal", TYPE)

  featureBuilder.add(envelope_Geometry1)
  featureCollection.add(featureBuilder.buildFeature(null))
  val featuresource1 = DataUtilities.source(featureCollection)
  val style1 = SLD.createSimpleStyle(featuresource1.getSchema, styleColor1)
  val layer1 = new FeatureLayer(featuresource1, style1)
  //  featureCollection.clear()

  featureBuilder.add(envelope_Geometry2)
  featureCollection.add(featureBuilder.buildFeature(null))
  val featuresource2 = DataUtilities.source(featureCollection)
  val style2 = SLD.createSimpleStyle(featuresource2.getSchema, styleColor2)
  val layer2 = new FeatureLayer(featuresource2, style2)
  //  featureCollection.clear()

  featureBuilder.add(envelope_Geometry3)
  featureCollection.add(featureBuilder.buildFeature(null))
  val featuresource3 = DataUtilities.source(featureCollection)
  val style3 = SLD.createSimpleStyle(featuresource3.getSchema, styleColor3)
  val layer3 = new FeatureLayer(featuresource3, style3)
  //  featureCollection.clear()

  featureBuilder.add(envelope_Geometry4)
  featureCollection.add(featureBuilder.buildFeature(null))
  val featuresource4 = DataUtilities.source(featureCollection)
  val style4 = SLD.createSimpleStyle(featuresource4.getSchema, styleColor4)
  val layer4 = new FeatureLayer(featuresource4, style4)
  //  featureCollection.clear()

  featureBuilder.add(envelope_Geometry5)
  featureCollection.add(featureBuilder.buildFeature(null))
  val featuresource5 = DataUtilities.source(featureCollection)
  val style5 = SLD.createSimpleStyle(featuresource5.getSchema, styleColor5)
  val layer5 = new FeatureLayer(featuresource5, style5)
  //  featureCollection.clear()

  featureBuilder.add(envelope_Geometry6)
  featureCollection.add(featureBuilder.buildFeature(null))
  val featuresource6 = DataUtilities.source(featureCollection)
  val style6 = SLD.createSimpleStyle(featuresource6.getSchema, styleColor6)
  val layer6 = new FeatureLayer(featuresource6, style6)

  //  val featuresource = DataUtilities.source(featureCollection)
  var map = new MapContent
  //  val style = SLD.createSimpleStyle(featuresource.getSchema, styleColor1)
  //  val layer = new FeatureLayer(featuresource, style)
  val shape2Image = new Vector2Image
  System.out.println("第一个")
  val styleColor7 = new Color(0, 0, 255)
  val parameters = shape2Image.addShapeLayer("/home/runxuan/data/Spatial_Compare/World/points_10M_wkt.csv", map, styleColor7, "multipolygon")
  map = parameters.map
  map.addLayer(layer6)
  map.addLayer(layer5)
  map.addLayer(layer4)
  map.addLayer(layer3)
  map.addLayer(layer2)
  map.addLayer(layer1)
  val bbox = Array[Double](parameters.bbox(0), parameters.bbox(1), parameters.bbox(2), parameters.bbox(3))
  val paser = new ParamPaser
  paser.setBbox(bbox)
  paser.setWidth(((bbox(2) - bbox(0)) * 5).toInt)
  paser.setHeight(((bbox(3) - bbox(1)) * 5).toInt)
  shape2Image.getMapContent(paser, "/home/runxuan/data/generated_img/COUNTY.png", map)
  JMapFrame.showMap(map)


}
