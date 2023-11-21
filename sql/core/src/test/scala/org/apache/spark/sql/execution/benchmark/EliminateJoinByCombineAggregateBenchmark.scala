/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.internal.SQLConf.ELIMINATE_JOIN_BY_COMBINE_AGGREGATE_ENABLED

/**
 * Benchmark to measure performance for [[EliminateJoinByCombineAggregate]] computation.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to
 *      "benchmarks/EliminateJoinByCombineAggregateBenchmark-results.txt".
 * }}}
 */
object EliminateJoinByCombineAggregateBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("EliminateJoinByCombineAggregate Computation") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath + "/ejbca_benchmark_table"
        val N = 1024 * 1024 * 20
        spark.range(0, N, 1, 11)
          .selectExpr("id as a", "id % 1024 as b", "id % 5 as c")
          .write.mode("overwrite")
          .parquet(path)

        val exprs = Seq(
          "avg(b) AS avg_b",
          "count(b) AS count_b",
          "count(distinct b) AS count_distinct_b")

        val df = spark.read.parquet(path)

        def f(step: Int = -1): Unit = {
          if (step == -1) {
            df.selectExpr(exprs: _*).join(
              df.selectExpr(exprs: _*).join(
                df.selectExpr(exprs: _*).join(
                  df.selectExpr(exprs: _*).join(
                    df.selectExpr(exprs: _*)
                  )
                )
              )
            ).noop()
          } else {
            val start1 = 1000000
            val start2 = 2000000
            val start3 = 3000000
            val start4 = 4000000
            val start5 = 5000000

            val df1 = df.where(s"a >= $start1 and a < ${start1 + step} and c between 2 and 4")
            val df2 = df.where(s"a >= $start2 and a < ${start2 + step} and c between 2 and 4")
            val df3 = df.where(s"a >= $start3 and a < ${start3 + step} and c between 2 and 4")
            val df4 = df.where(s"a >= $start4 and a < ${start4 + step} and c between 2 and 4")
            val df5 = df.where(s"a >= $start5 and a < ${start5 + step} and c between 2 and 4")

            df1.selectExpr(exprs: _*).join(
              df2.selectExpr(exprs: _*).join(
                df3.selectExpr(exprs: _*).join(
                  df4.selectExpr(exprs: _*).join(
                    df5.selectExpr(exprs: _*)
                  )
                )
              )
            ).noop()
          }
        }

        val benchmark = new Benchmark(
          "Benchmark EliminateJoinByCombineAggregate", N, minNumIters = 10, output = output)

        benchmark.addCase("filter is not defined, EliminateJoinByCombineAggregate: false") { _ =>
          f()
        }

        benchmark.addCase("filter is not defined, EliminateJoinByCombineAggregate: true") { _ =>
          withSQLConf(ELIMINATE_JOIN_BY_COMBINE_AGGREGATE_ENABLED.key -> "true") {
            f()
          }
        }

        Seq(1000000, 100000, 10000, 1000, 100, 10, 1).foreach { step =>
          benchmark.addCase(s"step is $step, EliminateJoinByCombineAggregate: false") { _ =>
            f(step)
          }

          benchmark.addCase(s"step is $step, EliminateJoinByCombineAggregate: true)") { _ =>
            withSQLConf(ELIMINATE_JOIN_BY_COMBINE_AGGREGATE_ENABLED.key -> "true") {
              f(step)
            }
          }
        }

        benchmark.run()
      }
    }
  }
}