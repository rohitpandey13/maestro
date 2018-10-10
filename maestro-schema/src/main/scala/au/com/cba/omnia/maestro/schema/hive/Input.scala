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
package au.com.cba.omnia.maestro.schema
package hive

import org.apache.hadoop.fs._
import scala.collection._


/** Produce a pipe from the given input path.
 *  If the path is a single file we stream the file.
 *  If the path is a directory we stream all files contained within it. */
object Input {

  /** List all input files associated with this name.
   *   If the name is a single file then just return that.
   *   If the name is a directory then return all non-directory files under it. */
  def list(name: String): List[Path] = {

    // File context represents the default HDFS file system.
    val context   = FileContext.getFileContext()
    val pathInput = new Path(name)

    trace(context, pathInput)

  }

  /** Trace all the input files starting from the given path. */
  def trace(context: FileContext, path: Path): List[Path] = {

    // Work out whether this is a single file or a directory.
    val statInput = context.getFileStatus(path)

    // For single files, return their names unharmed.
    if (path.getName.length == 0) {
      List()
    }
    else if (path.getName.substring(0, 1) == "_") {
      List()
    }

    else if (!statInput.isDirectory) {
      List(path)
    }

    // For directories, return all the regular files under them.
    else {
      val iterFiles = context.listStatus(path)
      val arrFiles  = mutable.ArrayBuffer[Path]()
      while (iterFiles.hasNext()) {
        arrFiles += iterFiles.next.getPath
      }

      arrFiles.result().toList
        .flatMap{ n => trace(context, n) }
    }
  }
}

