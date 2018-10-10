//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.task

import org.joda.time.{DateTime, DateTimeZone, Period}

import au.com.cba.omnia.thermometer.core.ThermometerSpec
import au.com.cba.omnia.thermometer.core.Thermometer._

import au.com.cba.omnia.omnitool.{Error, Result}

import au.com.cba.omnia.maestro.core.upload.{DataFile, DataFileTimestamped, Header, Trailer}

private object UploadExec extends UploadExecution

object UploadExecutionSpec extends ThermometerSpec { def is = s2"""

Upload execution properties
=========================

  can upload using execution monad                        $normal
  can custom upload using execution monad                 $custom
  can use continue to stop after an empty upload          $continue
  can upload sequence files for the same timestamp        $sequenceFiles

  can return the files matching the file pattern          $matching
  can upload the files and return upload info             $uploadFile

  won't upload files with hours with no time zone         $withHoursNoTZ
  can uploadUTC files with hours skipped in Sydney time   $withHoursUTC
  won't upload files for skipped daylight savings hours   $withHoursSkipped
  can return files and DateTimes for a UTC file pattern   $matchingUTC

  can parse  header and trailer and return right result   $headerTrailerParsing
  can parse header and return right result                $headerParsing
  can parse trailer and return right result               $trailerParsing
  can parse trailer with empty lines at the end of file   $trailerParsingWithEmptyLines
  parse garbage data and return result failure            $garbageParsing
"""

  def normal = withEnvironment(path(getClass.getResource("/upload-execution-normal").toString)) {
    val root      = s"$dir/user"
    val dirStruct = "normal/mydomain/mytable"
    val destDir   = s"hdfs-root/source/$dirStruct"
    val conf      = UploadConfig(
      s"$root/local-ingest/dataFeed/normal/mydomain", destDir,
      s"$root/local-archive/$dirStruct", s"hdfs-root/archive/$dirStruct", "mytable"
    )

    executesSuccessfully(UploadExec.upload(conf)).files must containTheSameElementsAs(List(
      s"$destDir/2014/11/11",
      s"$destDir/2014/11/12"
    ))
  }

  def custom = withEnvironment(path(getClass.getResource("/upload-execution-custom").toString)) {
    val root      = s"$dir/user"
    val destDir   = "hdfs-root/custom-landing"
    val conf      = UploadConfig(
      s"$root/local-ingest/custom", "hdfs-root/custom-landing", s"$root/local-archive/custom",
      "hdfs-root/custom-archive", "mytable", "{table}_{yyyyMMdd}.dat"
    )

    executesSuccessfully(UploadExec.upload(conf)).files must containTheSameElementsAs(List(
      s"$destDir/2014/11/11",
      s"$destDir/2014/11/12"
    ))
  }

  def continue = withEnvironment(path(getClass.getResource("/upload-execution-continue").toString)) {
    val root      = s"$dir/user"
    val dirStruct = "normal/mydomain/mytable"
    val conf      = UploadConfig(
      s"$root/local-ingest/dataFeed/normal/mydomain", s"hdfs-root/source/$dirStruct",
      s"$root/local-archive/$dirStruct", s"hdfs-root/archive/$dirStruct", "mytable"
    )
    val exec = for {
      res <- UploadExec.upload(conf)
      if res.continue
    } yield ()

    execute(exec).get must throwA[Throwable].like { case e => e.getMessage must startWith("Filter failed on") }
  }

  def sequenceFiles = withEnvironment(path(getClass.getResource("/upload-execution-sequenceFiles").toString)) {
    val root      = s"$dir/user"
    val dirStruct = "normal/mydomain/mytable"
    val destDir   = s"hdfs-root/source/$dirStruct"
    val conf      = UploadConfig(
      s"$root/local-ingest/dataFeed/normal/mydomain", destDir,
      s"$root/local-archive/$dirStruct", s"hdfs-root/archive/$dirStruct", "mytable"
    )

    executesSuccessfully(UploadExec.upload(conf)).files must containTheSameElementsAs(List(
      s"$destDir/2014/11/11"
    ))
  }

  def matching = withEnvironment(path(getClass.getResource("/find-sources").toString)) {
    val root      = s"$dir/user"
    val dirStruct = "normal/mydomain/mytable"
    val destDir   = s"hdfs-root/source/$dirStruct"
    val conf      = UploadConfig(
      s"$root/local-ingest/dataFeed/normal/mydomain", destDir,
      s"$root/local-archive/$dirStruct", s"hdfs-root/archive/$dirStruct", "mytable"
    )

    executesSuccessfully(UploadExec.findSources(conf)) must containTheSameElementsAs(List(
      DataFile(s"$root/local-ingest/dataFeed/normal/mydomain/mytable_20151021.dat","2015/10/21"),
      DataFile(s"$root/local-ingest/dataFeed/normal/mydomain/mytable_20151022.dat","2015/10/22")
    ))

  }

  def uploadFile = withEnvironment(path(getClass.getResource("/upload-sources").toString)) {
    val root      = s"$dir/user"
    val dirStruct = "normal/mydomain/mytable"
    val destDir   = s"hdfs-root/source/$dirStruct"
    val conf      = UploadConfig(
      s"$root/local-ingest/dataFeed/normal/mydomain", destDir,
      s"$root/local-archive/$dirStruct", s"hdfs-root/archive/$dirStruct", "mytable"
    )

    val src = List(
      DataFile(s"$root/local-ingest/dataFeed/normal/mydomain/mytable_20141113.dat", "2014/11/13"),
      DataFile(s"$root/local-ingest/dataFeed/normal/mydomain/mytable_20141114.dat", "2014/11/14")
    )

    executesSuccessfully(UploadExec.uploadSources(conf, src)).files must containTheSameElementsAs(List(
      s"$destDir/2014/11/13",
      s"$destDir/2014/11/14"
    ))
  }

  def headerTrailerParsing = withEnvironment(path(getClass.getResource("/ht-parser").toString)) {
    val path   = DataFile(s"$dir/user/local-ingest/dataFeed/normal/mydomain/mytable_20141118.dat", "")
    val result = Result.ok((Header("02-10-2014", "2:00:00 pm", "mytable_20141118.dat"), Trailer(4, "034", "name")))

    UploadExec.parseHT()(path) must_== result
  }

  def trailerParsing = withEnvironment(path(getClass.getResource("/trailer-parser").toString)) {
    val path   = DataFile(s"$dir/user/local-ingest/dataFeed/normal/mydomain/mytable_20141118.dat", "")
    val result = Result.ok(Trailer(4, "034", "name"))

    UploadExec.parseTrailer()(path) must_== result
  }

  def trailerParsingWithEmptyLines = withEnvironment(path(getClass.getResource("/trailer-parser-empty-lines").toString)) {
    val path   = DataFile(s"$dir/user/local-ingest/dataFeed/normal/mydomain/mytable_20141118.dat", "")
    val result = Result.ok(Trailer(4, "034", "name"))

    UploadExec.parseTrailer()(path) must_== result
  }

  def headerParsing = withEnvironment(path(getClass.getResource("/header-parser").toString)) {
    val path   = DataFile(s"$dir/user/local-ingest/dataFeed/normal/mydomain/mytable_20141118.dat", "")
    val result = Result.ok(Header("02-10-2014", "2:00:00 pm", "mytable_20141118.dat"))

    UploadExec.parseHeader()(path) must_== result
  }

  def garbageParsing = withEnvironment(path(getClass.getResource("/garbage-parser").toString)) {
    val path = DataFile(s"$dir/user/local-ingest/dataFeed/normal/mydomain/mytable_20141118.dat", "")

    UploadExec.parseHeader()(path) must beLike { case Error(_) => ok }
  }

  def withHoursUTC = withEnvironment(path(getClass.getResource("/upload-execution-with-hours").toString)) {
    val root      = s"$dir/user"
    val dirStruct = "normal/mydomain/mytable"
    val destDir   = s"hdfs-root/source/$dirStruct"
    val conf      = UploadConfig(
      s"$root/local-ingest/dataFeed/normal/mydomain", destDir,
      s"$root/local-archive/$dirStruct", s"hdfs-root/archive/$dirStruct", "mytable",
      "{table}_{yyyyMMddHH}.dat"
    )

    executesSuccessfully(UploadExec.uploadUTC(conf)).files must containTheSameElementsAs(List(
      s"$destDir/2016/10/02/02"
    ))
  }

  def withHoursNoTZ = withEnvironment(path(getClass.getResource("/upload-execution-with-hours").toString)) {
    val root      = s"$dir/user"
    val dirStruct = "normal/mydomain/mytable"
    val destDir   = s"hdfs-root/source/$dirStruct"
    val conf      = UploadConfig(
      s"$root/local-ingest/dataFeed/normal/mydomain", destDir,
      s"$root/local-archive/$dirStruct", s"hdfs-root/archive/$dirStruct", "mytable",
      "{table}_{yyyyMMddHH}.dat"
    )

    execute(UploadExec.upload(conf)).get must throwA[Throwable].like {
      case e => e.getMessage must startWith("Timestamps with hours are deprecated with upload and findSources")
    }
  }

  def withHoursSkipped = withEnvironment(path(getClass.getResource("/upload-execution-with-hours").toString)) {
    val root      = s"$dir/user"
    val dirStruct = "normal/mydomain/mytable"
    val destDir   = s"hdfs-root/source/$dirStruct"
    val conf      = UploadConfig(
      s"$root/local-ingest/dataFeed/normal/mydomain", destDir,
      s"$root/local-archive/$dirStruct", s"hdfs-root/archive/$dirStruct", "mytable",
      "{table}_{yyyyMMddHH}.dat"
    )

    execute(UploadExec.uploadTimestamped(conf, DateTimeZone.forID("Australia/Sydney"))).get must
    throwA[Throwable].like {
      case e => e.getMessage must startWith("""Cannot parse "mytable_2016100202.dat": Illegal instant due to time zone offset transition (Australia/Sydney)""")
    }
  }

  def matchingUTC = withEnvironment(path(getClass.getResource("/upload-execution-with-hours").toString)) {
    val root      = s"$dir/user"
    val dirStruct = "normal/mydomain/mytable"
    val destDir   = s"hdfs-root/source/$dirStruct"
    val conf      = UploadConfig(
      s"$root/local-ingest/dataFeed/normal/mydomain", destDir,
      s"$root/local-archive/$dirStruct", s"hdfs-root/archive/$dirStruct", "mytable",
      "{table}_{yyyyMMddHH}.dat"
    )

    executesSuccessfully(UploadExec.findSourcesUTC(conf)) must containTheSameElementsAs(List(
      DataFileTimestamped(
        s"$root/local-ingest/dataFeed/normal/mydomain/mytable_2016100202.dat","2016/10/02/02",
        new DateTime("2016-10-02T02:00", DateTimeZone.UTC),
        Period.hours(1)
      )
    ))
  }

}
