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

package au.com.cba.omnia.maestro.core
package upload

import java.io.File

import com.google.common.io.Files

import scalaz._, Scalaz._

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.omnitool.file.ops.Temp

import au.com.cba.omnia.permafrost.hdfs.Hdfs

/** Files copied to a directory on HDFS */
case class Copied(sources: List[File], dest: Path)

/** Functions to push files to HDFS */
object Push {

  /**
    * Copy files to HDFS and archive them.
    *
    * If nothing goes wrong, this method will:
    *   - copy the files to Hdfs
    *   - create an "ingestion complete" flag in each directory on Hdfs
    *   - archive the files
    *
    * This method will fail if a duplicate of the destination directories
    * already exist in HDFS, or if archived copies of the file already exist
    * locally or on HDFS.
    *
    * This method is not atomic. It assumes no other process is touching the
    * source or multiple destination files while this method runs.
    */
  def push(files: List[DataFile], destRoot: String, archRoot: String, hdfsArchRoot: String): Hdfs[List[Copied]] = {
    val fileGroups = files.groupBy(_.parsedDate).mapValues(subDirFiles => subDirFiles.map(f => new File(f.file))).toList

    fileGroups.traverse[Hdfs, Copied] { case (subDir, sources) =>
      pushToSubDir(sources, subDir, destRoot, archRoot, hdfsArchRoot)
    }
  }

  def pushToSubDir(sources: List[File], subDir: String, destRoot: String, archRoot: String, hdfsArchRoot: String): Hdfs[Copied] = {
    val destDir      = new Path(List(destRoot,     subDir) mkString File.separator)
    val archDir      = new File(List(archRoot,     subDir) mkString File.separator)
    val archHdfsDir  = new Path(List(hdfsArchRoot, subDir) mkString File.separator)
    val hdfsFlagFile = new Path(destDir, "_INGESTION_COMPLETE")

    for {
      // fail if the destination directory already exists or we would overwrite any archived files
      _ <- Hdfs.forbidden(Hdfs.exists(destDir), s"$destDir already exists")
      _ <- sources.traverse_(src => {
        val name         = archiveName(src)
        val archFile     = new File(archDir,     name)
        val archHdfsFile = new Path(archHdfsDir, name)
        for {
          _ <- Hdfs.prevent(archFile.exists, s"archive file destination $archFile already exists")
          _ <- Hdfs.forbidden(Hdfs.exists(archHdfsFile), s"archive file destination $archHdfsFile already exists")
        } yield ()
      })

      // push files everywhere and create flat
      _ <- Hdfs.mandatory(Hdfs.mkdirs(destDir), s"$destDir could not be created")
      _ <- sources.traverse_[Hdfs](src => Hdfs.copyFromLocalFile(src, destDir).map(_ => ()))
      _ <- Hdfs.create(hdfsFlagFile)
      _ <- sources.traverse_(src => archiveFile(src, archDir, archHdfsDir))
      _ <- sources.traverse_(src => Hdfs.guard(src.delete, s"could not delete ${src.getName} after processing"))
    } yield Copied(sources, destDir)
  }

  def archiveName(src: File) = src.getName + ".gz"

  /** Compress and archive file both locally and in hdfs */
  def archiveFile(raw: File, archDestDir: File, archHdfsDestDir: Path): Hdfs[Unit] =
    hdfsWithTempFile(raw, archiveName(raw), compressed => for {
      _ <- copyToHdfs(compressed, archHdfsDestDir)
      _ <- moveToDir(compressed, archDestDir)
    } yield () )

  def copyToHdfs(file: File, destDir: Path): Hdfs[Unit] =
    for {
      // mkdirs returns true if dir already exists
      _ <- Hdfs.mandatory(Hdfs.mkdirs(destDir), s"$destDir could not be created")
      _ <- Hdfs.copyFromLocalFile(file, destDir)
    } yield ()

  /** Move file to another directory. Convienient to run in a Hdfs operation. */
  def moveToDir(file: File, destDir: File): Hdfs[Unit] =
      for {
        _ <- Hdfs.guard(destDir.isDirectory || destDir.mkdirs, s"$destDir could not be created")
        _ <- Hdfs.value(Files.move(file, new File(destDir, file.getName)))
      } yield ()

  /** Convert a Hdfs operation that depends on a temporary file into a normal Hdfs operation */
  def hdfsWithTempFile[A](raw: File, fileName: String, action: File => Hdfs[A]): Hdfs[A] =
    Hdfs(c => Temp.withTempModified(
      raw, fileName, new GzipCompressorOutputStream(_), action(_).run(c)
    ))
}
