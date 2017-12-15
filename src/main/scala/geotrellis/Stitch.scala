package geotrellis

import scala.util.Try

import cats.implicits._
import com.monovore.decline._
import com.monovore.decline.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.kryo._
import geotrellis.spark.io.s3._
import geotrellis.vector.Extent
import org.apache.spark._
import org.apache.spark.serializer.KryoSerializer

/*
 run --bucket "rasterfoundry-staging-catalogs-us-east-1" --prefix "layers" --layer "e47d53d6-724c-4760-8fe3-07d85e8efc55" --zoom 7
 */

object Stitch extends CommandApp(

  name   = "gt-stitch",
  header = "Turn a GeoTrellis layer into a GeoTiff",
  main   = {

    val readExtent: String => Option[Extent] = { s =>
      s.split(",").toList.traverse(n => Try(n.toInt).toOption).flatMap {
        case xmn :: ymn :: xmx :: ymx :: _ => Some(Extent(xmn.toDouble, ymn.toDouble, xmx.toDouble, ymx.toDouble))
        case _ => None
      }
    }

    /* Ensures that only positive, non-zero values can be given as arguments. */
    type UInt = Int Refined Positive

    val bucketO: Opts[String] = Opts.option[String]("bucket", help = "S3 bucket.")
    val prefixO: Opts[String] = Opts.option[String]("prefix", help = "Path to GeoTrellis layer catalog.")
    val layerO:  Opts[String] = Opts.option[String]("layer",  help = "Name of the layer.")
    val zoomO:   Opts[UInt]   = Opts.option[UInt]("zoom",     help = "Zoom level of the layer to stitch.")
    val tiffO:   Opts[String] = Opts.option[String]("tiff",   help = "Name of the TIFF to be output.").withDefault("stiched.tiff")
    val extentO: Opts[Option[Extent]] = Opts.option[String]("extent", help = "Extent to bound the layer query (LatLng).").orNone.map(_ >>= readExtent)

    (bucketO, prefixO, layerO, zoomO, tiffO, extentO).mapN { (bucket, prefix, layer, zoom, tiff, extent) =>

      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("GeoTrellis Stitch")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

      implicit val sc = new SparkContext(conf)

      val store  = S3AttributeStore(bucket, prefix)
      val reader = S3LayerReader(store)

      val lid = LayerId(layer, zoom.value)

      val tiles: MultibandTileLayerRDD[SpatialKey] = extent match {
        case None    => reader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](lid)
        case Some(e) =>
          val crs: CRS = store.readMetadata[TileLayerMetadata[SpatialKey]](lid).crs

          reader
            .query[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](lid)
            .where(Intersects(e.reproject(LatLng, crs)))
            .result
      }

      MultibandGeoTiff(tiles.stitch, tiles.metadata.crs).write(tiff)

      sc.stop()

      println("Done.")
    }
  }
)
