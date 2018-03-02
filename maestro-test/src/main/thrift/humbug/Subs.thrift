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

#@namespace scala au.com.cba.omnia.maestro.test.thrift.humbug

struct SubOne {
  1: string  stringField
  2: bool    booleanField
}

struct SubTwo {
  1: i32     intField
  2: i64     longField
  3: double  doubleField
}

struct SubThree {
  1: i64     longField
  2: i32     intField
}

struct SubFour {
  1: string field1000
  2: string field10
  3: string field100
}

struct Unsub {
  1: string otherStringField
}

struct WrongType {
  1: string booleanField
}
