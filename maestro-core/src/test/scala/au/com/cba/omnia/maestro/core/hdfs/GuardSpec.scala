package au.com.cba.omnia.maestro.core.hdfs

import org.specs2.matcher.FileMatchers

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, Thermometer}, Thermometer._

class GuardSpec extends ThermometerSpec with FileMatchers { def is = s2"""

HDFS Guard properties
=====================

  expandPaths:
    matches globbed dirs               $matchGlobbedDirs
    skips files                        $skipFiles
    skips processed dirs               $skipProcessed

  expandTransferredPaths:
    skips uningested dirs              $skipUningested

  listNonEmptyFiles:
    lists non-empty files              $listNonEmptyFiles
    skips subdirectories               $skipSubdirectories

  createFlagFile:
    creates _PROCESSED                 $createPROCESSED
"""

  def matchGlobbedDirs = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandPaths(s"$dir/user/a*") must containTheSameElementsAs(List(
        s"file:$dir/user/a",
        s"file:$dir/user/a1"
        // excludes various other directories that don't match the glob
      ))
    }
  }

  def skipFiles = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandPaths(s"$dir/user/b*") must containTheSameElementsAs(List(
        s"file:$dir/user/b1"
        // excludes "b2" because it's not a directory
      ))
    }
  }

  def skipProcessed = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandPaths(s"$dir/user/c*") must containTheSameElementsAs(List(
        s"file:$dir/user/c",
        s"file:$dir/user/c_transferred"
        // excludes "c_processed"
      ))
    }
  }

  def skipUningested = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandTransferredPaths(s"$dir/user/c*") must containTheSameElementsAs(List(
        s"file:$dir/user/c_transferred"
        // excludes "c"
        // excludes "c_processed"
      ))
    }
  }

  def listNonEmptyFiles = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      val paths = List(s"$dir/user/a", s"$dir/user/c")
      Guard.listNonEmptyFiles(paths) must containTheSameElementsAs(List(
        s"file:$dir/user/c/c.dat"
        // excludes "a/a.dat" because it is zero-length
      ))
    }
  }

  def skipSubdirectories = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      val paths = List(s"$dir/user")
      Guard.listNonEmptyFiles(paths) must containTheSameElementsAs(List(
        s"file:$dir/user/b2"
        // excludes all subdirectories
      ))
    }
  }

  def createPROCESSED = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      val paths = List(s"$dir/user/a1", s"$dir/user/b1")
      Guard.createFlagFile(paths)
      s"$dir/user/a1/_PROCESSED" must beAFilePath
      s"$dir/user/b1/_PROCESSED" must beAFilePath
    }
  }
}
