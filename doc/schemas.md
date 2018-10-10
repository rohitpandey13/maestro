
# The Maestro Schema System (Buzzgrind)

The Commonwealth Bank of Australia, like many other large institutions, owns
hundreds of terabytes worth of financial data whose schema and semantics
(meaning) are encoded implicitly in the implementation of its various software
systems. The format of the data produced by systems which manage payments,
customer information, fraud, credit risk and so on is described informally in
human readable documents or spreadsheets, rather than in a machine readable
form. 

For a data warehousing group in such an institution, the task of importing new
tables with hundreds poorly described columns, all encoded as ASCII strings, is
a weekly occurrence. Critically, the lack of a formal schema for imported data
means that data quality is both difficult to measure and tends to degrade over
time. Rows become corrupted due to race conditions in concurrent processes,
lost due to forgotten loads, and source systems change in a way that was not
anticipated by follow-on processes, causing bad output.

Building on related work by Fisher [1, 2] and Scaffidi [3] the maestro
schema system provides tooling to assign types to table columns by scanning
through the data and matching each value against a set of known classifiers.
The classifiers are predicates that detect encodings of data values commonly
found in financial institutions, for example: account numbers, currency codes,
dates, internal system identifiers and so on. The novel aspect to this work is
the algorithm which takes the histogram of how many values in a column match
each classifier, and infers a type that accurately describes the column. 

Building a table schema is a four step process:

1. Acquire a .names file containing column names and storage types. The storage
   type for a column is the binary format used to encode the values. Typical
   storage types are 'string', 'int', and 'double'. If the table is already
   accessable from a front-end database like Hive, then this names file can 
   be obtained directly from the database.

2. Scan through sample data to produce a classifier histogram for each column.
   This produces a .taste file for the table. A classifier is a function that
   matches some useful subset of the values that can be encoded by the storage
   type. For example, we have classifiers that match dates, currency codes and
   positive integers.  The ultimate job of the inference system is recover
   these more informative types.

3. Using the .names and .taste files, infer a skeleton table .schema file.
   The schema contains types for the columns which can have their types
   automatically inferred, and a place holder for un-inferrable types.

4. Manually inspect the resulting schema, and fill in un-inferrable types.

Table data can then be checked against the schema. Rows that do not match the
schema are written to an erroneous rows file, along with diagnostic information.

[1] PADS: a domain specific language for processing ad-hoc data
    Kathleen Fisher, Robert Gruber
    Programming Langauge Design and Implementation, 2005

[2] The next 700 data description languages
    Kathleen Fisher, Yitzhak Mandelbaum, David Walker
    Journal of the ACM, Volume 57, Number 2, 2010.

[3] Topes: Reusable abstractions for validating data
    Christopher Scaffidi, Brad Myers, Mary Shaw
    International Conference on Software Engineering, 2008


## Step 1. Name Acquisition

Suppose we have a table named 'accounts' that contains the opening and closing
dates for a set of savings accounts. The first step is to build a names file
(`accounts.names`) that contains one line for each column, where the first word
in each line is the column name and the second is the Hive storage type.
For example:

```
account_name string
open_date    string
close_date   string
active       string
process_id   int
```

If there is an existing table in Hive, then the names file can be obtained
directly from the Hive metastore:

```
hive -e "describe accounts" > accounts.names
```

Trailing words on the line are ignored, such as field comments.


## Step 2. Tasting

The "tasting" process reads sample table data and produces a histogram
describing what sort of data appears in each column. This process produces a
new file, eg `accounts.taste`. Use the following job to do this:

```
hadoop jar ${SCHEMAS_JAR} com.twitter.scalding.Tool \
    au.com.cba.omnia.maestro.schema.jobs.Taste \
    --hdfs \
    --in-local-names ${LOCAL_NAMES} \
    --in-hdfs-data   ${HDFS_DATA}   \
    --out-hdfs-taste ${HDFS_TASTE}  \
```

* `LOCAL_NAMES` local path to the file holding column names and hive storage
  types, eg the `accounts.names` file in the previous example.
* `HDFS_DATA`  hdfs path to either a single block of input data, or a
  directory that contains data blocks. 
* `HDFS_TASTE` hdfs path where the output taste file should be written. 

The resulting taste file is a JSON encoded list containing extracted metadata
for each column. Here is a simple example where most values in the column were
dates, except for some missing data represented as whitespace:

```
{ "name":        "eff_date", 
  "storage":     "string", 
  "classifiers": { "Any": 185091, "Day.DDcMMcYYYY('.')": 185081, "White":10 }, 
  "sample": 
    { "maxSize": 100, "spilled": 0, 
      "histogram":{ "08.09.2014": 184790, "05.09.2014": 120, "04.09.2014": 18, 
                    " ": 10,              "29.08.2014": 4,   "03.09.2014": 4 } } 
}
```

The `name` and `storage` fields are the information from the input names file.

The classifiers field contains a set of tuples which record how many values in
the column match each of the known classifiers. The maestro schemas framework
includes a fixed set of classifiers, though it is easy to add new ones.

For classifiers whose names include a period, such as `Day.DDcMMcYYYYY('.')`, 
the part before the period (`Day`) refers to a tope, which is a named entity 
in the world. The part after the period (`DDcMMcYYYY('.')`) refers to the 
syntax used to represent the tope. In this case we have a physical Day being
represented as day, month and year numbers separated by periods, like  
"1965.05.02".

Classifiers whose names do not include a period are raw syntaxes that do not
nessesarally refer to a tope. For example, `White` matches white space
characters, `AlphaNum` matches alphanumeric strings, `Upper` matches upper
case characters, and so on. Although the data that matches the `Upper` syntax
may have some more specific meaning, we are not always able to recognize it. 

The `Any` classifier matches all strings, so in the above example the
classifier set { "Any":185091, "Day.DDcMMcYYYY('.')":185081, "White":10 } 
shows that we had classifiers for all values in the column.


## Step 3. Inference

The inference process takes both the names and taste file and produces a
skeleton schema. Use the following command:

```
java -cp ${SCHEMAS_JAR} au.com.cba.omnia.maestro.schema.commands.Infer \
    --database ${DATABASE_NAME} \
    --table    ${TABLE_NAME}    \
    --names    ${LOCAL_NAMES}   \
    --taste    ${LOCAL_TASTE} > ${LOCAL_SCHEMA}
```

* `DATABASE_NAME` name of the database.
* `TABLE_NAME`    name of the table.
* `LOCAL_NAMES`   local path to names file produced in step 1 above. 
* `LOCAL_TASTE`   local path to taste file produced in step 2 above.
* `LOCAL_SCHEMA`  local path for output schema file.

Given the `accounts.names` and `account.taste` files above, we get the
following schema:

```
{ "name"       : "eff_date", 
  "storage"    : "string", 
  "format"     : "Day.DDcMMcYYYY('.') + White", 
  "classifiers": { "Day.DDcMMcYYYY('.')": 185081, " ":10  } 
}
```

The schema file contains one element for each column in the input table, where
the format field gives the inferred type for the column. The inference process
uses heuristics to choose a suitable format. In the above example it was easy
because all the values in the column were Day strings or whitespace.

Here is a larger example that contains three separate classifiers:

```
{ "name"        : "lra_logodds_app",
  "storage"     : "string",
  "format"      : "White + Real + Null",
  "classifiers" : { "Null":3172796, "White":354790, "Real":3929927 }
}
```

Here is one where the inference process did not succeed:

```
{ "name"     : "appraisal_date_1", 
  "storage"  : "string", 
  "format"   : "-", 
  "histogram": { "Any": 185091, "White": 183372, "Day.DDcMMcYYYY('.')": 473 } }
```

In the above case, the count on the Any classifier reveals that there were 
185091  values in total. However, 183372 were whitespace and only 473 were days.
As the sum of the white space values and day values does not equal the total
number of values in the column, we cannot infer a type for this column. There
is no type other than Any that matches all values.


## Step 4. Fill in Missing Types

If the inferencer heuristics were not able to infer a value type for a column
then the format field will be set to the place holder value "-". This will
happen if the values in a column match multiple classifiers, but no classifier
matches them all. 

In these cases the user needs to either manually update the schema with the
desired type, improve the classifier in the maestro implementation, or fix the 
input table in the case of corrupted data.

More details of the inference process are given in the [Inferencer Heuristics]
section below.


## Checking

The final step is to check the data against the constructed table schema. Use
the following command:

```
hadoop jar ${SCHEMAS_JAR} com.twitter.scalding.Tool \
    au.com.cba.omnia.maestro.schema.jobs.Check \
    --hdfs \
    --in-schema       ${LOCAL_SCHEMA} \
    --in-hdfs-data    ${HDFS_DATA} \
    --out-hdfs-errors ${HDFS_ERRORS} \
    --out-hdfs-diag   ${HDFS_DIAG}
```

* `LOCAL_SCHEMA`  local path for input schema file.
* `HDFS_DATA`     hdfs path to either a single block of input data, or a
  directory that contains data blocks. This data is checked against the schema.
* `HDFS_ERRORS`   hdfs path to write a copy of the rows that do not match
  the schema.
* `HDFS_DIAG`     hdfs path to write error diagnostic information. For every 
  error row written to `HDFS_ERRORS`, we get a diagnostic row describing why
  the error row did not match the schema.


## Inferencer Heuristics

The classifiers for each syntax are arranged in a subtyping hierarchy, like so:

![Classifiers]
(https://github.com/CommBank/maestro/blob/master/doc/schemas/hierarchy.png)

At the top, the Any classifier matches all strings. Each classifier matches all
the strings that are matched by its descendents. Conversely, all strings
matched by a classifier are also matched by its ancestors. Note that the
hierarchy is a graph rather than a tree, as strings which match the Nat
classifier (natural numbers) also match AlphaNum and PosReal -- so a child can
have more than one parent.

In the diagram, the 'i' subscript indicates that the classifier is an isolate,
meaning that the strings matched by that classifier are not matched by any
other classifier. For example, the empty string is matched by the Empty
classifier only. 

Edges annotated by a bullet indicate that the associated child classifiers
completely partition the parent classifier. For example, all strings that match
the Real classifier also match either PosReal or NegReal, but not both at the
same time. In this case we say that the PosReal and NegReal classifiers are
*separate*.

The inference process takes the histogram of how many values in a column match
each classifier, and produces a type. The process has three phases.


#### Remove redundant parent counts from the histogram.

For some parent node with count N, if it has a set of child nodes that are
mutually separate, and the the sum of the counts for these child nodes is N,
then remove the count for the parent.

For example, if we have Digits:100, Exact("0"):40, Exact("1"):60, then remove
Digits:100. As parent classifiers are guaranteed to match all values that their
children do, then the count of the parent is implied by the count of the
children.

If the child nodes are not separate then this process does not work. For
example, if we had the histogram Any:100, AlphaNum:40, Real:60 we cannot remove
Any:100 because there are strings that match both the AlphaNum and Real
classifiers -- eg "555". Likewise, there are strings that match the Any
classifier, but neither AlphaNum or Real -- eg "N/AVAIL".


#### Remove uninteresting child counts from the histogram.

For some parent node with count N, if there are child nodes that are partitions
of the parent, and all the partitions have counts, and the sum of those counts
is N, then we can remove *all* child nodes reachable from the parent. 

For example, if we have Real:100, PosReal:70 and NegReal:30, we only keep
Real:100. The fact that a certain percentage of values are Positive or Negative
is useful information in itself, but we do not need it to produce a sum type
for the associated values. All we need is the name of the set that contains all
the values in the column, and in this case Real will suffice.


#### Extract a sum type from the resulting histogram.

If the final histogram contains a single entry, then we can use the associated
classifier as the type of the column. For example, if we have Real:100 then we
use the type Real.

If the histogram contains multiple entries that are all siblings, then we
collect the associated classifiers into a sum type. For example, if we have
Real:100, White:200, Null:5 then we use the type Real + White + Null.

If neither of the above cases match then a type cannot be inferred for the
column. This happens when some values in a column match multiple classifiers,
but there is no inferable type (besides Any) that matches them all. 


#### Common causes for failed inference are:

1. The column contains values that match an existing classifier, but the values
   should not have the associated type. For example, if a column contains the
   first names of people we may see a single 'AUD' (Audrey) value, but that
   same string can also be classified as a currency code (Australian Dollars).
   The currency code classifier will not match all the first names of people,
   so type inference will fail.

2. The classifiers do not match the full range of values of the associated type.
   For example, if a classifier for financial system codes matched 'HLS' (home
   loan system) and 'SAV' (savings accounts), but the column also contains
   an unmatched code 'FOR' (foreign exchange).

3. The data is dirty or corrupted. For example, if most values in a value have
   the form YYYY.MM.DD, but there one that is '1900.01.'

