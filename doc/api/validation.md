Validation API
==============

Validators can be specified on a record (`Validator.by`) or field
basis (`Validator.of`). The most common scenario will be using
pre-baked `Check`s on a field basis.

To use checks (as with most of the maesto APIs) just import the api:

```scala
import au.com.cba.omnia.maestro.api._
```


Field level validators and checks
---------------------------------

Checks are the simplest form of Validator. They have specialized typed, implementations
and error messages that should provide _standard_ validation behaviour.

Checks can be defined / used stand alone, or in the context of field validation:

```scala
// check that a string is not empty
Check.nonempty

// or the more likely scenario, applying that in the context of a specific field
Validator.of(Field.Name, Check.nonempty)

// check that a string is one of a set of values
Check.oneOf("M", "F", "O")

// or again, in a field context
Vaildator.of(Field.Gender, Check.oneOf("M", "F", "O"))
```


Record Validation
-----------------

When the built in checks are not enough, or when you need to cross
validate fields it is possible to construct record level validators.

The first and most common way to define a check on a record is by using
`Validator.by`:

```scala

// If we have a customer record
case class Customer(id: String, classifier: String, name: String)

// then want to validate that the customer id always starts with the "classifer value"
val validator =
  Validator.by[Customer](customer => customer.id.startsWith(customer.classifier), "classification")

// to run this (normally done internally to other maestro calls) we would call run

// a success example would be
validator.run(Customer("X0001", "X", "Fred")) == Success(Customer("X0001", "X", "Fred"))

// a failure example would be
validator.run(Customer("Y0001", "Z", "Barney")) == Failure(NonEmptyList("classification"))

```
