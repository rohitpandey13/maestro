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

package au.com.cba.omnia.maestro.benchmark.thrift

import org.scalameter.api.Gen

/** Contains generators for thrift types */
object Generators extends Serializable {
  def mkStruct10Values(rows: Int): Array[Struct10] =
    (0 until rows).map { row =>
      val x = new Struct10
      x.str1   = s"r$row-f0"
      x.str2   = s"r$row-f1"
      x.dbl3   = row * 10 + 2.5
      x.int4   = row * 10 + 3
      x.long5  = Int.MaxValue + row * 10 + 4
      x.str6   = s"r$row-f5"
      x.str7   = s"r$row-f6"
      x.dbl8   = row * 10 + 7.5
      x.int9   = row * 10 + 8
      x.long10 = Int.MaxValue + row * 10 + 9

      x
    }.toArray

  def mkStruct20Values(rows: Int): Array[Struct20] =
    (0 until rows).map { row =>
      val x = new Struct20
      x.str1   = s"r$row-f0"
      x.str2   = s"r$row-f1"
      x.dbl3   = row * 20 + 2.5
      x.int4   = row * 20 + 3
      x.long5  = Int.MaxValue + row * 20 + 4
      x.str6   = s"r$row-f5"
      x.str7   = s"r$row-f6"
      x.dbl8   = row * 20 + 7.5
      x.int9   = row * 20 + 8
      x.long10 = Int.MaxValue + row * 20 + 9
      x.str11  = s"r$row-f10"
      x.str12  = s"r$row-f11"
      x.dbl13  = row * 20 + 12.5
      x.int14  = row * 20 + 13
      x.long15 = Int.MaxValue + row * 20 + 14
      x.str16  = s"r$row-f15"
      x.str17  = s"r$row-f16"
      x.dbl18  = row * 20 + 17.5
      x.int19  = row * 20 + 18
      x.long20 = Int.MaxValue + row * 20 + 19

      x
    }.toArray

  def mkStruct30Values(rows: Int): Array[Struct30] =
    (0 until rows).map { row =>
      val x = new Struct30
      x.str1   = s"r$row-f0"
      x.str2   = s"r$row-f1"
      x.dbl3   = row * 30 + 2.5
      x.int4   = row * 30 + 3
      x.long5  = Int.MaxValue + row * 30 + 4
      x.str6   = s"r$row-f5"
      x.str7   = s"r$row-f6"
      x.dbl8   = row * 30 + 7.5
      x.int9   = row * 30 + 8
      x.long10 = Int.MaxValue + row * 30 + 9
      x.str11  = s"r$row-f10"
      x.str12  = s"r$row-f11"
      x.dbl13  = row * 30 + 12.5
      x.int14  = row * 30 + 13
      x.long15 = Int.MaxValue + row * 30 + 14
      x.str16  = s"r$row-f15"
      x.str17  = s"r$row-f16"
      x.dbl18  = row * 30 + 17.5
      x.int19  = row * 30 + 18
      x.long20 = Int.MaxValue + row * 30 + 19
      x.str21  = s"r$row-f20"
      x.str22  = s"r$row-f21"
      x.dbl23  = row * 30 + 22.5
      x.int24  = row * 30 + 23
      x.long25 = Int.MaxValue + row * 30 + 24
      x.str26  = s"r$row-f25"
      x.str27  = s"r$row-f26"
      x.dbl28  = row * 30 + 27.5
      x.int29  = row * 30 + 28
      x.long30 = Int.MaxValue + row * 30 + 29

      x
    }.toArray

  def mkStruct500Values(rows: Int): Array[Struct500] =
    (0 until rows).map { row =>
      val x = new Struct500
      x.str1    = s"r$row-f0"
      x.str2    = s"r$row-f1"
      x.dbl3    = row * 500 + 2.5
      x.int4    = row * 500 + 3
      x.long5   = Int.MaxValue + row * 500 + 4
      x.str6    = s"r$row-f5"
      x.str7    = s"r$row-f6"
      x.dbl8    = row * 500 + 7.5
      x.int9    = row * 500 + 8
      x.long10  = Int.MaxValue + row * 500 + 9
      x.str11   = s"r$row-f10"
      x.str12   = s"r$row-f11"
      x.dbl13   = row * 500 + 12.5
      x.int14   = row * 500 + 13
      x.long15  = Int.MaxValue + row * 500 + 14
      x.str16   = s"r$row-f15"
      x.str17   = s"r$row-f16"
      x.dbl18   = row * 500 + 17.5
      x.int19   = row * 500 + 18
      x.long20  = Int.MaxValue + row * 500 + 19
      x.str21   = s"r$row-f20"
      x.str22   = s"r$row-f21"
      x.dbl23   = row * 500 + 22.5
      x.int24   = row * 500 + 23
      x.long25  = Int.MaxValue + row * 500 + 24
      x.str26   = s"r$row-f25"
      x.str27   = s"r$row-f26"
      x.dbl28   = row * 500 + 27.5
      x.int29   = row * 500 + 28
      x.long30  = Int.MaxValue + row * 500 + 29
      x.str31   = s"r$row-f30"
      x.str32   = s"r$row-f31"
      x.dbl33   = row * 500 + 32.5
      x.int34   = row * 500 + 33
      x.long35  = Int.MaxValue + row * 500 + 34
      x.str36   = s"r$row-f35"
      x.str37   = s"r$row-f36"
      x.dbl38   = row * 500 + 37.5
      x.int39   = row * 500 + 38
      x.long40  = Int.MaxValue + row * 500 + 39
      x.str41   = s"r$row-f40"
      x.str42   = s"r$row-f41"
      x.dbl43   = row * 500 + 42.5
      x.int44   = row * 500 + 43
      x.long45  = Int.MaxValue + row * 500 + 44
      x.str46   = s"r$row-f45"
      x.str47   = s"r$row-f46"
      x.dbl48   = row * 500 + 47.5
      x.int49   = row * 500 + 48
      x.long50  = Int.MaxValue + row * 500 + 49
      x.str51   = s"r$row-f50"
      x.str52   = s"r$row-f51"
      x.dbl53   = row * 500 + 52.5
      x.int54   = row * 500 + 53
      x.long55  = Int.MaxValue + row * 500 + 54
      x.str56   = s"r$row-f55"
      x.str57   = s"r$row-f56"
      x.dbl58   = row * 500 + 57.5
      x.int59   = row * 500 + 58
      x.long60  = Int.MaxValue + row * 500 + 59
      x.str61   = s"r$row-f60"
      x.str62   = s"r$row-f61"
      x.dbl63   = row * 500 + 62.5
      x.int64   = row * 500 + 63
      x.long65  = Int.MaxValue + row * 500 + 64
      x.str66   = s"r$row-f65"
      x.str67   = s"r$row-f66"
      x.dbl68   = row * 500 + 67.5
      x.int69   = row * 500 + 68
      x.long70  = Int.MaxValue + row * 500 + 69
      x.str71   = s"r$row-f70"
      x.str72   = s"r$row-f71"
      x.dbl73   = row * 500 + 72.5
      x.int74   = row * 500 + 73
      x.long75  = Int.MaxValue + row * 500 + 74
      x.str76   = s"r$row-f75"
      x.str77   = s"r$row-f76"
      x.dbl78   = row * 500 + 77.5
      x.int79   = row * 500 + 78
      x.long80  = Int.MaxValue + row * 500 + 79
      x.str81   = s"r$row-f80"
      x.str82   = s"r$row-f81"
      x.dbl83   = row * 500 + 82.5
      x.int84   = row * 500 + 83
      x.long85  = Int.MaxValue + row * 500 + 84
      x.str86   = s"r$row-f85"
      x.str87   = s"r$row-f86"
      x.dbl88   = row * 500 + 87.5
      x.int89   = row * 500 + 88
      x.long90  = Int.MaxValue + row * 500 + 89
      x.str91   = s"r$row-f90"
      x.str92   = s"r$row-f91"
      x.dbl93   = row * 500 + 92.5
      x.int94   = row * 500 + 93
      x.long95  = Int.MaxValue + row * 500 + 94
      x.str96   = s"r$row-f95"
      x.str97   = s"r$row-f96"
      x.dbl98   = row * 500 + 97.5
      x.int99   = row * 500 + 98
      x.long100 = Int.MaxValue + row * 500 + 99
      x.str101  = s"r$row-f100"
      x.str102  = s"r$row-f101"
      x.dbl103  = row * 500 + 102.5
      x.int104  = row * 500 + 103
      x.long105 = Int.MaxValue + row * 500 + 104
      x.str106  = s"r$row-f105"
      x.str107  = s"r$row-f106"
      x.dbl108  = row * 500 + 107.5
      x.int109  = row * 500 + 108
      x.long110 = Int.MaxValue + row * 500 + 109
      x.str111  = s"r$row-f110"
      x.str112  = s"r$row-f111"
      x.dbl113  = row * 500 + 112.5
      x.int114  = row * 500 + 113
      x.long115 = Int.MaxValue + row * 500 + 114
      x.str116  = s"r$row-f115"
      x.str117  = s"r$row-f116"
      x.dbl118  = row * 500 + 117.5
      x.int119  = row * 500 + 118
      x.long120 = Int.MaxValue + row * 500 + 119
      x.str121  = s"r$row-f120"
      x.str122  = s"r$row-f121"
      x.dbl123  = row * 500 + 122.5
      x.int124  = row * 500 + 123
      x.long125 = Int.MaxValue + row * 500 + 124
      x.str126  = s"r$row-f125"
      x.str127  = s"r$row-f126"
      x.dbl128  = row * 500 + 127.5
      x.int129  = row * 500 + 128
      x.long130 = Int.MaxValue + row * 500 + 129
      x.str131  = s"r$row-f130"
      x.str132  = s"r$row-f131"
      x.dbl133  = row * 500 + 132.5
      x.int134  = row * 500 + 133
      x.long135 = Int.MaxValue + row * 500 + 134
      x.str136  = s"r$row-f135"
      x.str137  = s"r$row-f136"
      x.dbl138  = row * 500 + 137.5
      x.int139  = row * 500 + 138
      x.long140 = Int.MaxValue + row * 500 + 139
      x.str141  = s"r$row-f140"
      x.str142  = s"r$row-f141"
      x.dbl143  = row * 500 + 142.5
      x.int144  = row * 500 + 143
      x.long145 = Int.MaxValue + row * 500 + 144
      x.str146  = s"r$row-f145"
      x.str147  = s"r$row-f146"
      x.dbl148  = row * 500 + 147.5
      x.int149  = row * 500 + 148
      x.long150 = Int.MaxValue + row * 500 + 149
      x.str151  = s"r$row-f150"
      x.str152  = s"r$row-f151"
      x.dbl153  = row * 500 + 152.5
      x.int154  = row * 500 + 153
      x.long155 = Int.MaxValue + row * 500 + 154
      x.str156  = s"r$row-f155"
      x.str157  = s"r$row-f156"
      x.dbl158  = row * 500 + 157.5
      x.int159  = row * 500 + 158
      x.long160 = Int.MaxValue + row * 500 + 159
      x.str161  = s"r$row-f160"
      x.str162  = s"r$row-f161"
      x.dbl163  = row * 500 + 162.5
      x.int164  = row * 500 + 163
      x.long165 = Int.MaxValue + row * 500 + 164
      x.str166  = s"r$row-f165"
      x.str167  = s"r$row-f166"
      x.dbl168  = row * 500 + 167.5
      x.int169  = row * 500 + 168
      x.long170 = Int.MaxValue + row * 500 + 169
      x.str171  = s"r$row-f170"
      x.str172  = s"r$row-f171"
      x.dbl173  = row * 500 + 172.5
      x.int174  = row * 500 + 173
      x.long175 = Int.MaxValue + row * 500 + 174
      x.str176  = s"r$row-f175"
      x.str177  = s"r$row-f176"
      x.dbl178  = row * 500 + 177.5
      x.int179  = row * 500 + 178
      x.long180 = Int.MaxValue + row * 500 + 179
      x.str181  = s"r$row-f180"
      x.str182  = s"r$row-f181"
      x.dbl183  = row * 500 + 182.5
      x.int184  = row * 500 + 183
      x.long185 = Int.MaxValue + row * 500 + 184
      x.str186  = s"r$row-f185"
      x.str187  = s"r$row-f186"
      x.dbl188  = row * 500 + 187.5
      x.int189  = row * 500 + 188
      x.long190 = Int.MaxValue + row * 500 + 189
      x.str191  = s"r$row-f190"
      x.str192  = s"r$row-f191"
      x.dbl193  = row * 500 + 192.5
      x.int194  = row * 500 + 193
      x.long195 = Int.MaxValue + row * 500 + 194
      x.str196  = s"r$row-f195"
      x.str197  = s"r$row-f196"
      x.dbl198  = row * 500 + 197.5
      x.int199  = row * 500 + 198
      x.long200 = Int.MaxValue + row * 500 + 199
      x.str201  = s"r$row-f200"
      x.str202  = s"r$row-f201"
      x.dbl203  = row * 500 + 202.5
      x.int204  = row * 500 + 203
      x.long205 = Int.MaxValue + row * 500 + 204
      x.str206  = s"r$row-f205"
      x.str207  = s"r$row-f206"
      x.dbl208  = row * 500 + 207.5
      x.int209  = row * 500 + 208
      x.long210 = Int.MaxValue + row * 500 + 209
      x.str211  = s"r$row-f210"
      x.str212  = s"r$row-f211"
      x.dbl213  = row * 500 + 212.5
      x.int214  = row * 500 + 213
      x.long215 = Int.MaxValue + row * 500 + 214
      x.str216  = s"r$row-f215"
      x.str217  = s"r$row-f216"
      x.dbl218  = row * 500 + 217.5
      x.int219  = row * 500 + 218
      x.long220 = Int.MaxValue + row * 500 + 219
      x.str221  = s"r$row-f220"
      x.str222  = s"r$row-f221"
      x.dbl223  = row * 500 + 222.5
      x.int224  = row * 500 + 223
      x.long225 = Int.MaxValue + row * 500 + 224
      x.str226  = s"r$row-f225"
      x.str227  = s"r$row-f226"
      x.dbl228  = row * 500 + 227.5
      x.int229  = row * 500 + 228
      x.long230 = Int.MaxValue + row * 500 + 229
      x.str231  = s"r$row-f230"
      x.str232  = s"r$row-f231"
      x.dbl233  = row * 500 + 232.5
      x.int234  = row * 500 + 233
      x.long235 = Int.MaxValue + row * 500 + 234
      x.str236  = s"r$row-f235"
      x.str237  = s"r$row-f236"
      x.dbl238  = row * 500 + 237.5
      x.int239  = row * 500 + 238
      x.long240 = Int.MaxValue + row * 500 + 239
      x.str241  = s"r$row-f240"
      x.str242  = s"r$row-f241"
      x.dbl243  = row * 500 + 242.5
      x.int244  = row * 500 + 243
      x.long245 = Int.MaxValue + row * 500 + 244
      x.str246  = s"r$row-f245"
      x.str247  = s"r$row-f246"
      x.dbl248  = row * 500 + 247.5
      x.int249  = row * 500 + 248
      x.long250 = Int.MaxValue + row * 500 + 249
      x.str251  = s"r$row-f250"
      x.str252  = s"r$row-f251"
      x.dbl253  = row * 500 + 252.5
      x.int254  = row * 500 + 253
      x.long255 = Int.MaxValue + row * 500 + 254
      x.str256  = s"r$row-f255"
      x.str257  = s"r$row-f256"
      x.dbl258  = row * 500 + 257.5
      x.int259  = row * 500 + 258
      x.long260 = Int.MaxValue + row * 500 + 259
      x.str261  = s"r$row-f260"
      x.str262  = s"r$row-f261"
      x.dbl263  = row * 500 + 262.5
      x.int264  = row * 500 + 263
      x.long265 = Int.MaxValue + row * 500 + 264
      x.str266  = s"r$row-f265"
      x.str267  = s"r$row-f266"
      x.dbl268  = row * 500 + 267.5
      x.int269  = row * 500 + 268
      x.long270 = Int.MaxValue + row * 500 + 269
      x.str271  = s"r$row-f270"
      x.str272  = s"r$row-f271"
      x.dbl273  = row * 500 + 272.5
      x.int274  = row * 500 + 273
      x.long275 = Int.MaxValue + row * 500 + 274
      x.str276  = s"r$row-f275"
      x.str277  = s"r$row-f276"
      x.dbl278  = row * 500 + 277.5
      x.int279  = row * 500 + 278
      x.long280 = Int.MaxValue + row * 500 + 279
      x.str281  = s"r$row-f280"
      x.str282  = s"r$row-f281"
      x.dbl283  = row * 500 + 282.5
      x.int284  = row * 500 + 283
      x.long285 = Int.MaxValue + row * 500 + 284
      x.str286  = s"r$row-f285"
      x.str287  = s"r$row-f286"
      x.dbl288  = row * 500 + 287.5
      x.int289  = row * 500 + 288
      x.long290 = Int.MaxValue + row * 500 + 289
      x.str291  = s"r$row-f290"
      x.str292  = s"r$row-f291"
      x.dbl293  = row * 500 + 292.5
      x.int294  = row * 500 + 293
      x.long295 = Int.MaxValue + row * 500 + 294
      x.str296  = s"r$row-f295"
      x.str297  = s"r$row-f296"
      x.dbl298  = row * 500 + 297.5
      x.int299  = row * 500 + 298
      x.long300 = Int.MaxValue + row * 500 + 299
      x.str301  = s"r$row-f300"
      x.str302  = s"r$row-f301"
      x.dbl303  = row * 500 + 302.5
      x.int304  = row * 500 + 303
      x.long305 = Int.MaxValue + row * 500 + 304
      x.str306  = s"r$row-f305"
      x.str307  = s"r$row-f306"
      x.dbl308  = row * 500 + 307.5
      x.int309  = row * 500 + 308
      x.long310 = Int.MaxValue + row * 500 + 309
      x.str311  = s"r$row-f310"
      x.str312  = s"r$row-f311"
      x.dbl313  = row * 500 + 312.5
      x.int314  = row * 500 + 313
      x.long315 = Int.MaxValue + row * 500 + 314
      x.str316  = s"r$row-f315"
      x.str317  = s"r$row-f316"
      x.dbl318  = row * 500 + 317.5
      x.int319  = row * 500 + 318
      x.long320 = Int.MaxValue + row * 500 + 319
      x.str321  = s"r$row-f320"
      x.str322  = s"r$row-f321"
      x.dbl323  = row * 500 + 322.5
      x.int324  = row * 500 + 323
      x.long325 = Int.MaxValue + row * 500 + 324
      x.str326  = s"r$row-f325"
      x.str327  = s"r$row-f326"
      x.dbl328  = row * 500 + 327.5
      x.int329  = row * 500 + 328
      x.long330 = Int.MaxValue + row * 500 + 329
      x.str331  = s"r$row-f330"
      x.str332  = s"r$row-f331"
      x.dbl333  = row * 500 + 332.5
      x.int334  = row * 500 + 333
      x.long335 = Int.MaxValue + row * 500 + 334
      x.str336  = s"r$row-f335"
      x.str337  = s"r$row-f336"
      x.dbl338  = row * 500 + 337.5
      x.int339  = row * 500 + 338
      x.long340 = Int.MaxValue + row * 500 + 339
      x.str341  = s"r$row-f340"
      x.str342  = s"r$row-f341"
      x.dbl343  = row * 500 + 342.5
      x.int344  = row * 500 + 343
      x.long345 = Int.MaxValue + row * 500 + 344
      x.str346  = s"r$row-f345"
      x.str347  = s"r$row-f346"
      x.dbl348  = row * 500 + 347.5
      x.int349  = row * 500 + 348
      x.long350 = Int.MaxValue + row * 500 + 349
      x.str351  = s"r$row-f350"
      x.str352  = s"r$row-f351"
      x.dbl353  = row * 500 + 352.5
      x.int354  = row * 500 + 353
      x.long355 = Int.MaxValue + row * 500 + 354
      x.str356  = s"r$row-f355"
      x.str357  = s"r$row-f356"
      x.dbl358  = row * 500 + 357.5
      x.int359  = row * 500 + 358
      x.long360 = Int.MaxValue + row * 500 + 359
      x.str361  = s"r$row-f360"
      x.str362  = s"r$row-f361"
      x.dbl363  = row * 500 + 362.5
      x.int364  = row * 500 + 363
      x.long365 = Int.MaxValue + row * 500 + 364
      x.str366  = s"r$row-f365"
      x.str367  = s"r$row-f366"
      x.dbl368  = row * 500 + 367.5
      x.int369  = row * 500 + 368
      x.long370 = Int.MaxValue + row * 500 + 369
      x.str371  = s"r$row-f370"
      x.str372  = s"r$row-f371"
      x.dbl373  = row * 500 + 372.5
      x.int374  = row * 500 + 373
      x.long375 = Int.MaxValue + row * 500 + 374
      x.str376  = s"r$row-f375"
      x.str377  = s"r$row-f376"
      x.dbl378  = row * 500 + 377.5
      x.int379  = row * 500 + 378
      x.long380 = Int.MaxValue + row * 500 + 379
      x.str381  = s"r$row-f380"
      x.str382  = s"r$row-f381"
      x.dbl383  = row * 500 + 382.5
      x.int384  = row * 500 + 383
      x.long385 = Int.MaxValue + row * 500 + 384
      x.str386  = s"r$row-f385"
      x.str387  = s"r$row-f386"
      x.dbl388  = row * 500 + 387.5
      x.int389  = row * 500 + 388
      x.long390 = Int.MaxValue + row * 500 + 389
      x.str391  = s"r$row-f390"
      x.str392  = s"r$row-f391"
      x.dbl393  = row * 500 + 392.5
      x.int394  = row * 500 + 393
      x.long395 = Int.MaxValue + row * 500 + 394
      x.str396  = s"r$row-f395"
      x.str397  = s"r$row-f396"
      x.dbl398  = row * 500 + 397.5
      x.int399  = row * 500 + 398
      x.long400 = Int.MaxValue + row * 500 + 399
      x.str401  = s"r$row-f400"
      x.str402  = s"r$row-f401"
      x.dbl403  = row * 500 + 402.5
      x.int404  = row * 500 + 403
      x.long405 = Int.MaxValue + row * 500 + 404
      x.str406  = s"r$row-f405"
      x.str407  = s"r$row-f406"
      x.dbl408  = row * 500 + 407.5
      x.int409  = row * 500 + 408
      x.long410 = Int.MaxValue + row * 500 + 409
      x.str411  = s"r$row-f410"
      x.str412  = s"r$row-f411"
      x.dbl413  = row * 500 + 412.5
      x.int414  = row * 500 + 413
      x.long415 = Int.MaxValue + row * 500 + 414
      x.str416  = s"r$row-f415"
      x.str417  = s"r$row-f416"
      x.dbl418  = row * 500 + 417.5
      x.int419  = row * 500 + 418
      x.long420 = Int.MaxValue + row * 500 + 419
      x.str421  = s"r$row-f420"
      x.str422  = s"r$row-f421"
      x.dbl423  = row * 500 + 422.5
      x.int424  = row * 500 + 423
      x.long425 = Int.MaxValue + row * 500 + 424
      x.str426  = s"r$row-f425"
      x.str427  = s"r$row-f426"
      x.dbl428  = row * 500 + 427.5
      x.int429  = row * 500 + 428
      x.long430 = Int.MaxValue + row * 500 + 429
      x.str431  = s"r$row-f430"
      x.str432  = s"r$row-f431"
      x.dbl433  = row * 500 + 432.5
      x.int434  = row * 500 + 433
      x.long435 = Int.MaxValue + row * 500 + 434
      x.str436  = s"r$row-f435"
      x.str437  = s"r$row-f436"
      x.dbl438  = row * 500 + 437.5
      x.int439  = row * 500 + 438
      x.long440 = Int.MaxValue + row * 500 + 439
      x.str441  = s"r$row-f440"
      x.str442  = s"r$row-f441"
      x.dbl443  = row * 500 + 442.5
      x.int444  = row * 500 + 443
      x.long445 = Int.MaxValue + row * 500 + 444
      x.str446  = s"r$row-f445"
      x.str447  = s"r$row-f446"
      x.dbl448  = row * 500 + 447.5
      x.int449  = row * 500 + 448
      x.long450 = Int.MaxValue + row * 500 + 449
      x.str451  = s"r$row-f450"
      x.str452  = s"r$row-f451"
      x.dbl453  = row * 500 + 452.5
      x.int454  = row * 500 + 453
      x.long455 = Int.MaxValue + row * 500 + 454
      x.str456  = s"r$row-f455"
      x.str457  = s"r$row-f456"
      x.dbl458  = row * 500 + 457.5
      x.int459  = row * 500 + 458
      x.long460 = Int.MaxValue + row * 500 + 459
      x.str461  = s"r$row-f460"
      x.str462  = s"r$row-f461"
      x.dbl463  = row * 500 + 462.5
      x.int464  = row * 500 + 463
      x.long465 = Int.MaxValue + row * 500 + 464
      x.str466  = s"r$row-f465"
      x.str467  = s"r$row-f466"
      x.dbl468  = row * 500 + 467.5
      x.int469  = row * 500 + 468
      x.long470 = Int.MaxValue + row * 500 + 469
      x.str471  = s"r$row-f470"
      x.str472  = s"r$row-f471"
      x.dbl473  = row * 500 + 472.5
      x.int474  = row * 500 + 473
      x.long475 = Int.MaxValue + row * 500 + 474
      x.str476  = s"r$row-f475"
      x.str477  = s"r$row-f476"
      x.dbl478  = row * 500 + 477.5
      x.int479  = row * 500 + 478
      x.long480 = Int.MaxValue + row * 500 + 479
      x.str481  = s"r$row-f480"
      x.str482  = s"r$row-f481"
      x.dbl483  = row * 500 + 482.5
      x.int484  = row * 500 + 483
      x.long485 = Int.MaxValue + row * 500 + 484
      x.str486  = s"r$row-f485"
      x.str487  = s"r$row-f486"
      x.dbl488  = row * 500 + 487.5
      x.int489  = row * 500 + 488
      x.long490 = Int.MaxValue + row * 500 + 489
      x.str491  = s"r$row-f490"
      x.str492  = s"r$row-f491"
      x.dbl493  = row * 500 + 492.5
      x.int494  = row * 500 + 493
      x.long495 = Int.MaxValue + row * 500 + 494
      x.str496  = s"r$row-f495"
      x.str497  = s"r$row-f496"
      x.dbl498  = row * 500 + 497.5
      x.int499  = row * 500 + 498
      x.long500 = Int.MaxValue + row * 500 + 499

      x
    }.toArray

  def mkRow(v: Product): List[String] =
    v.productIterator.map(_.toString).toList

  /** Generator for the number of rows we benchmark */
  val rows: Gen[Int] =
    Gen.range("rows")(10000, 30000, 5000)

  /** Generator for [[Struct10]]s */
  val struct10Values: Gen[Array[Struct10]] =
    rows.map(mkStruct10Values(_))

  /** Generator for [[Struct20]]s */
  val struct20Values: Gen[Array[Struct20]] =
    rows.map(mkStruct20Values(_))

  /** Generator for [[Struct30]]s */
  val struct30Values: Gen[Array[Struct30]] =
    rows.map(mkStruct30Values(_))

  /** Generator for [[Struct500]]s */
  val struct500Values: Gen[Array[Struct500]] =
    Gen.enumeration("rows")(10000, 30000).map(mkStruct500Values(_))

  /** Generator for rows which can be parsed into [[Struct10]]s */
  val struct10Rows: Gen[Array[List[String]]] =
    struct10Values.map(_.map(mkRow))

  /** Generator for rows which can be parsed into [[Struct20]]s */
  val struct20Rows: Gen[Array[List[String]]] =
    struct20Values.map(_.map(mkRow))

  /** Generator for rows which can be parsed into [[Struct30]]s */
  val struct30Rows: Gen[Array[List[String]]] =
    struct30Values.map(_.map(mkRow))

  /** Generator for rows which can be parsed into [[Struct30]]s */
  val struct500Rows: Gen[Array[List[String]]] =
    struct500Values.map(_.map(mkRow))
}
