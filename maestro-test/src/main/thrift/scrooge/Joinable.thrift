#   Copyright 2014 Commonwealth Bank of Australia
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

#@namespace scala au.com.cba.omnia.maestro.test.thrift.scrooge

struct JoinOneScrooge {
  1: string  stringField
  2: bool    booleanField
  3: i32     someField
}

struct JoinOneIncompatibleScrooge {
  1: string  someField
}

struct JoinOneDuplicateScrooge {
  1: i32     someField
}

struct JoinTwoScrooge {
  1: string  stringField
  2: i64     longField
  3: double  doubleField
}

struct JoinableScrooge {
  1: bool    booleanField
  2: i64     longField
  3: i32     someField
}

struct UnjoinableScrooge {
  1: string  missingField
}

struct UnjoinableScrooge2 {
  1: string  missingField
  2: string  missingField2
}
