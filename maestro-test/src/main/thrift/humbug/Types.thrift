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

struct Types {
  1: string  stringField
  2: bool    booleanField
  3: i32     intField
  4: i64     longField
  5: double  doubleField
  6: optional i32 optIntField
  7: optional string optStringField
}
