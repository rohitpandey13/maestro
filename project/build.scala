//   Copyright 2014-2018 Commonwealth Bank of Australia
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

import sbt._
import Keys._


import com.twitter.scrooge.ScroogeSBT._

import sbtassembly.AssemblyPlugin.autoImport.assembly

import sbtunidoc.Plugin.{ScalaUnidoc, UnidocKeys}
import UnidocKeys.{unidoc, unidocProjectFilter}

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._
import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._

import au.com.cba.omnia.humbug.HumbugSBT._

object build extends Build {
  type Sett = Def.Setting[_]

  val thermometerVersion = "1.6.10-20190624015541-be82ab7"
  val ebenezerVersion    = "0.24.8-20190310174013-ab54bd4"
  val beeswaxVersion     = "0.2.10-20190630132403-526f304"
  val omnitoolVersion    = "1.15.8-20190310152130-00578df"
  val permafrostVersion  = "0.15.8-20190310163117-ca25070"
  val edgeVersion        = "3.8.8-20190310174050-3ad51ac"
  val humbugVersion      = "0.8.7-20190306212707-48e6182"
  val parlourVersion     = "1.14.0-20190730012817-2f8925e"

  lazy val standardSettings: Seq[Sett] =
    Defaults.coreDefaultSettings ++
    uniformDependencySettings ++
    strictDependencySettings ++
    uniform.docSettings("https://github.com/CommBank/maestro") ++
    Seq(
      logLevel in assembly := Level.Error,
      updateOptions := updateOptions.value.withCachedResolution(true),
      // Run tests sequentially across the subprojects.
      concurrentRestrictions in Global := Seq(
        Tags.limit(Tags.Test, 1)
      )
    )

  lazy val all = Project(
    id = "all"
  , base = file(".")
  , settings =
       standardSettings
    ++ uniform.project("maestro-all", "au.com.cba.omnia.maestro")
    ++ uniform.ghsettings
    ++ Seq[Sett](
         publishArtifact := false
       , addCompilerPlugin(depend.macroParadise())
       , unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject -- inProjects(example, schema, benchmark)
    )
  , aggregate = Seq(core, macros, scalding, api, test, schema)
  )

  lazy val api = Project(
    id = "api"
  , base = file("maestro-api")
  , settings =
       standardSettings
    ++ uniform.project("maestro", "au.com.cba.omnia.maestro.api")
    ++ Seq[Sett](
      libraryDependencies ++= depend.hadoopClasspath ++ depend.hadoop() ++ depend.testing()
    )
  ).dependsOn(core)
   .dependsOn(macros)
   .dependsOn(scalding)

  lazy val core = Project(
    id = "core"
  , base = file("maestro-core")
  , settings =
       standardSettings
    ++ uniformThriftSettings
    ++ uniform.project("maestro-core", "au.com.cba.omnia.maestro.core")
    ++ humbugSettings
    ++ Seq[Sett](
      scroogeThriftSourceFolder in Test := sourceDirectory.value / "test" / "thrift" / "scrooge",
      humbugThriftSourceFolder in Test := sourceDirectory.value / "test" / "thrift" / "humbug",
      libraryDependencies ++=
           depend.scalaz()
        ++ depend.hadoopClasspath
        ++ depend.hadoop()
        ++ depend.shapeless() ++ depend.testing()
        ++ depend.omnia("beeswax",       beeswaxVersion)
        ++ depend.omnia("ebenezer",      ebenezerVersion)
        ++ depend.omnia("ebenezer-test", ebenezerVersion, "test")
        ++ depend.omnia("permafrost",    permafrostVersion)
        ++ depend.omnia("edge",          edgeVersion)
        ++ depend.omnia("humbug-core",   humbugVersion)
        ++ depend.omnia("omnitool-time", omnitoolVersion)
        ++ depend.omnia("omnitool-file", omnitoolVersion)
        ++ depend.omnia("parlour",       parlourVersion)
        ++ depend.scalikejdbc()
        ++ Seq(
          noHadoop("commons-validator"  % "commons-validator" % "1.4.0"),
          "com.opencsv"                 % "opencsv"           % "3.3"
            exclude ("org.apache.commons", "commons-lang3") // conflicts with hive
        ),
      parallelExecution in Test := false
    )
  )

  lazy val macros = Project(
    id = "macros"
  , base = file("maestro-macros")
  , settings =
       standardSettings
    ++ uniform.project("maestro-macros", "au.com.cba.omnia.maestro.macros")
    ++ Seq[Sett](
         libraryDependencies ++= Seq(
           "org.scala-lang" % "scala-reflect" % scalaVersion.value
         ) ++ depend.testing()
       , addCompilerPlugin(depend.macroParadise())
    )
  ).dependsOn(core)
   .dependsOn(test % "test")

  lazy val scalding = Project(
    id = "scalding"
  , base = file("maestro-scalding")
  , settings =
       standardSettings
    ++ uniformThriftSettings
    ++ uniform.project("maestro-scalding", "au.com.cba.omnia.maestro.scalding")
    ++ Seq[Sett](
      libraryDependencies ++=
           depend.scalaz()
        ++ depend.scalding()
        ++ depend.hadoopClasspath
        ++ depend.hadoop()
        ++ depend.parquet()
        ++ depend.testing()
        ++ depend.omnia("omnitool-core", omnitoolVersion, "test").map(_ classifier "tests")
        ++ depend.omnia("thermometer-hive", thermometerVersion, "test"),
      parallelExecution in Test := false
    )
  ).dependsOn(core % "compile->compile;test->test")

  lazy val schema = Project(
    id = "schema"
  , base = file("maestro-schema")
  , settings =
       standardSettings
    ++ uniform.project("maestro-schema", "au.com.cba.omnia.maestro.schema")
    ++ uniformAssemblySettings
    ++ Seq[Sett](
          libraryDependencies ++= Seq(
            "com.quantifind"         %% "sumac"                    % "0.3.0"
          , "org.scala-lang"         %  "scala-reflect"            % scalaVersion.value
          , "org.apache.commons"     %  "commons-lang3"            % "3.1"
          ) ++ depend.scalding() ++ depend.hadoopClasspath ++ depend.hadoop()
       )
    )

  lazy val example = Project(
    id = "example"
  , base = file("maestro-example")
  , settings =
       standardSettings
    ++ uniform.project("maestro-example", "au.com.cba.omnia.maestro.example")
    ++ uniformAssemblySettings
    ++ uniformThriftSettings
    ++ Seq[Sett](
         libraryDependencies ++= depend.hadoopClasspath ++ depend.hadoop() ++ depend.parquet() ++
           depend.scalikejdbc().map(_.copy(configurations = Some("test")))
       , parallelExecution in Test := false
       , sources in doc in Compile := List()
       , addCompilerPlugin(depend.macroParadise())
    )
  ).dependsOn(core)
   .dependsOn(macros)
   .dependsOn(api)
   .dependsOn(test % "test")

  lazy val benchmark = Project(
    id = "benchmark"
  , base = file("maestro-benchmark")
  , settings =
       standardSettings
    ++ uniform.project("maestro-benchmark", "au.com.cba.omnia.maestro.benchmark")
    ++ humbugSettings
    ++ Seq[Sett](
      libraryDependencies ++= Seq(
        "com.storm-enroute" %% "scalameter" % "0.6"
          exclude("org.scala-lang.modules", "scala-parser-combinators_2.11")
          exclude("org.scala-lang.modules", "scala-xml_2.11")
      ) ++ depend.testing()
    , testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
    , parallelExecution in Test := false
    , logBuffered := false
    )
  ).dependsOn(core)
   .dependsOn(macros)
   .dependsOn(api)

  lazy val test = Project(
    id = "test"
  , base = file("maestro-test")
  , settings =
       standardSettings
    ++ uniform.project("maestro-test", "au.com.cba.omnia.maestro.test")
    ++ uniformThriftSettings
    ++ humbugSettings
    ++ Seq[Sett](
         scroogeThriftSourceFolder in Compile := sourceDirectory.value / "main" / "thrift" / "scrooge"
       , humbugThriftSourceFolder  in Compile := sourceDirectory.value / "main" / "thrift" / "humbug"
       , libraryDependencies ++=
           depend.omnia("ebenezer-test",    ebenezerVersion)
           ++ depend.hadoopClasspath ++ depend.hadoop()
           ++ depend.testing(configuration = "test")
    )
  ).dependsOn(core, scalding)
}
