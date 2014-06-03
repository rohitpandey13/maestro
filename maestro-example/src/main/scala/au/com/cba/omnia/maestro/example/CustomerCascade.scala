package au.com.cba.omnia.maestro.example

import scalaz.{Tag => _, _}, Scalaz._

import com.twitter.scalding._, TDsl._

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.core.codec._

import au.com.cba.omnia.maestro.example.thrift._

class CustomerCascade(args: Args) extends Job(args) with MaestroSupport[Customer] {
  val env           = args("env")
  val domain        = "customer"
  val inputs        = Guard.expandPaths(s"${env}/source/${domain}/*")
  val outbound      = s"${env}/outbound/${domain}"
  val analytics     = s"${env}/view/warehouse/${domain}"
  val dateView      = s"${env}/view/warehouse/${domain}/by-date"
  val catView       = s"${env}/view/warehouse/${domain}/by-cat"
  val errors        = s"${env}/errors/${domain}"

  val cleaners      = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )

  val validators    = Validator.all(
    Validator.of(Fields.CustomerSubCat, Check.oneOf("M", "F")),
    Validator.by[Customer](_.customerAcct.length == 4, "Customer accounts should always be a length of 4")
  )

  val filter        = RowFilter.keep

  load[Customer]("|", inputs, errors, Maestro.now(), cleaners, validators, filter) |>
    (view(Partition.byDate(Fields.EffectiveDate), dateView) _ &&&
     view(Partition.byFields2(Fields.CustomerCat, Fields.CustomerSubCat), catView))
}

class SplitCustomerCascade(args: Args) extends Job(args) with MaestroSupport[Customer] {
  val env           = args("env")
  val domain        = "customer"
  val inputs        = Guard.expandPaths(s"${env}/source/${domain}/*")
  val outbound      = s"${env}/outbound/${domain}"
  val analytics     = s"${env}/view/warehouse/${domain}"
  val dateView      = s"${env}/view/warehouse/${domain}/by-date"
  val catView       = s"${env}/view/warehouse/${domain}/by-cat"
  val errors        = s"${env}/errors/${domain}"
  
  val cleaners      = Clean.all(
    Clean.trim,
    Clean.removeNonPrintables
  )

  val validators    = Validator.all(
    Validator.of(Fields.CustomerSubCat, Check.oneOf("M", "F")),
    Validator.by[Customer](_.customerAcct.length == 4, "Customer accounts should always be a length of 4")
  )

  val filter        = RowFilter.keep
  
  load[Customer]("|", inputs, errors, Maestro.now(), cleaners, validators, filter)
    .map(split[Customer,(Customer, Customer, Customer)]) >>*
  (
    view(Partition.byDate(Fields.EffectiveDate), dateView),
    view(Partition.byFields2(Fields.CustomerCat, Fields.CustomerSubCat), catView),
    view(Partition.byDate(Fields.EffectiveDate), dateView)
  )
}