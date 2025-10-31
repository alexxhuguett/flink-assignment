import java.text.SimpleDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.Protocol.{Commit, CommitGeo, CommitSummary}
import util.{CommitGeoParser, CommitParser}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.PatternSelectFunction

import java.util.Date

/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignment {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
      * Setups the streaming environment including loading and parsing of the datasets.
      *
      * DO NOT TOUCH!
      */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
    dummy_question(commitStream).print()

    /** Start the streaming environment. **/
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[String] = {
    input.map(_.sha)
  }

  /**
    * Write a Flink application which outputs the sha of commits with at least 20 additions.
    * Output format: sha
    */
  def question_one(input: DataStream[Commit]): DataStream[String] = {
    input
      .filter(_.stats.exists(_.additions >= 20))
      .map(_.sha)
  }

  /**
    * Write a Flink application which outputs the names of the files with more than 30 deletions.
    * Output format:  fileName
    */
  def question_two(input: DataStream[Commit]): DataStream[String] = {
    input
      .flatMap(_.files)
      .filter(_.deletions > 30)
      .flatMap(_.filename)
  }

  /**
    * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
    * Output format: (fileExtension, #occurrences)
    */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input
      .flatMap(_.files)
      .flatMap(_.filename)
      .flatMap { name =>
        val ext = name.split("\\.").lastOption.getOrElse("")
        ext match {
          case "java"  => Some(("java", 1))
          case "scala" => Some(("scala", 1))
          case _       => None
        }
      }
      .keyBy(_._1)
      .sum(1)
  }

  /**
    * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
    * Output format: (extension, status, count)
    */
  def question_four(
      input: DataStream[Commit]): DataStream[(String, String, Int)] = {
    input
      .flatMap(commit => commit.files)
      .filter(file => file.filename.exists(name => name.endsWith(".js") || name.endsWith(".py")))
      .map{file =>
        val ext = if (file.filename.exists(_.endsWith(".js"))) ".js" else ".py"
        val status = file.status.getOrElse("unknown")
        (ext, status, file.changes)
      }
      .keyBy(f => (f._1, f._2))
      .sum(2)
  }

  /**
    * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
    * Make use of a non-keyed window.
    * Output format: (date, count)
    */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Commit] {
        override def extractAscendingTimestamp(c: Commit): Long =
          c.commit.committer.date.getTime
      })
      .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
      .apply { (window: TimeWindow, elems: Iterable[Commit], out: Collector[(String, Int)]) =>
        val fmt = new SimpleDateFormat("dd-MM-yyyy")
        val date = fmt.format(new Date(window.getStart))
        out.collect((date, elems.size))
      }
  }

  /**
    * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
    * Compute every 12 hours the amount of small and large commits in the last 48 hours.
    * Output format: (type, count)
    */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Commit] {
        override def extractAscendingTimestamp(c: Commit): Long =
          c.commit.committer.date.getTime
      })
      .map { commit =>
        val total = commit.stats.map(_.total).getOrElse(0)
        val commitType = if (total > 20) "large" else "small"
        (commitType, 1)
      }
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
      .sum(1)
  }

  /**
    * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
    *
    * The fields of this case class:
    *
    * repo: name of the repo.
    * date: use the start of the window in format "dd-MM-yyyy".
    * amountOfCommits: the number of commits on that day for that repository.
    * amountOfCommitters: the amount of unique committers contributing to the repository.
    * totalChanges: the sum of total changes in all commits.
    * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
    *
    * Hint: Write your own ProcessWindowFunction.
    * Output format: CommitSummary
    */
  def question_seven(
      commitStream: DataStream[Commit]): DataStream[CommitSummary] = {
    def extractRepo(url: String): String = {
      val cleaned = url.replaceAll("\\?.*$", "")
      val parts = cleaned.split("/")
      val idxRepos = parts.indexOf("repos")
      if (idxRepos >= 0 && idxRepos + 2 < parts.length) {
        s"${parts(idxRepos + 1)}/${parts(idxRepos + 2)}"
      } else if (parts.length >= 4) {
        s"${parts(parts.length - 3)}/${parts(parts.length - 2)}"
      } else cleaned
    }

    commitStream
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Commit] {
        override def extractAscendingTimestamp(c: Commit): Long =
          c.commit.committer.date.getTime
      })
      .keyBy(c => extractRepo(c.url))
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .process(new org.apache.flink.streaming.api.scala.function.ProcessWindowFunction[
        Commit, CommitSummary, String, TimeWindow
      ] {
        override def process(
                              repo: String,
                              ctx: Context,
                              elements: Iterable[Commit],
                              out: Collector[CommitSummary]
                            ): Unit = {
          val fmt  = new SimpleDateFormat("dd-MM-yyyy")
          val date = fmt.format(new Date(ctx.window.getStart))

          var amountOfCommits     = 0
          var totalChanges        = 0
          val commitsPerCommitter = scala.collection.mutable.Map.empty[String, Int].withDefaultValue(0)

          elements.foreach { c =>
            amountOfCommits += 1
            totalChanges += c.stats.map(_.total).getOrElse(0)
            val committerName = c.commit.committer.name
            commitsPerCommitter.update(committerName, commitsPerCommitter(committerName) + 1)
          }

          val amountOfCommitters = commitsPerCommitter.size

          if (amountOfCommits > 20 && amountOfCommitters <= 2) {
            val maxCommits = if (commitsPerCommitter.isEmpty) 0 else commitsPerCommitter.values.max
            val topCommitter =
              commitsPerCommitter
                .collect { case (name, cnt) if cnt == maxCommits => name }
                .toList
                .sorted
                .mkString(",")

            out.collect(
              CommitSummary(
                repo              = repo,
                date              = date,
                amountOfCommits   = amountOfCommits,
                amountOfCommitters= amountOfCommitters,
                totalChanges      = totalChanges,
                mostPopularCommitter = topCommitter
              )
            )
          }
        }
      })
  }

  /**
    * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
    * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
    * Get the weekly amount of changes for the java files (.java extension) per continent. If no java files are changed in a week, no output should be shown that week.
    *
    * Hint: Find the correct join to use!
    * Output format: (continent, amount)
    */
  def question_eight(
                      commitStream: DataStream[Commit],
                      geoStream: DataStream[CommitGeo]
                    ): DataStream[(String, Int)] = {

    val commitsPerFile: DataStream[(String, util.Protocol.File)] =
      commitStream
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Commit] {
          override def extractAscendingTimestamp(c: Commit): Long =
            c.commit.committer.date.getTime
        })
        .flatMap { commit =>
          commit.files.map(file => (commit.sha, file))
        }(org.apache.flink.api.scala.createTypeInformation[(String, util.Protocol.File)])
        .filter { t: (String, util.Protocol.File) =>
          t._2.filename.exists(_.endsWith(".java"))
        }

    val geoWithTs: DataStream[CommitGeo] =
      geoStream
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[CommitGeo] {
          override def extractAscendingTimestamp(g: CommitGeo): Long =
            g.createdAt.getTime
        })

    commitsPerFile
      .keyBy(_._1)
      .intervalJoin(geoWithTs.keyBy(_.sha))
      .between(Time.hours(-1), Time.minutes(30))
      .process(new ProcessJoinFunction[(String, util.Protocol.File), CommitGeo, (String, Int)] {
        override def processElement(
                                     left: (String, util.Protocol.File),
                                     geo: CommitGeo,
                                     ctx: ProcessJoinFunction[(String, util.Protocol.File), CommitGeo, (String, Int)]#Context,
                                     out: Collector[(String, Int)]
                                   ): Unit = {
          out.collect((geo.continent, left._2.changes))
        }
      })
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .sum(1)
  }

  /**
    * Find all files that were added and removed within one day. Output as (repository, filename).
    *
    * Hint: Use the Complex Event Processing library (CEP).
    * Output format: (repository, filename)
    */
  def question_nine(
      inputStream: DataStream[Commit]): DataStream[(String, String)] = {



    val fileEvents: DataStream[(String, String, String, Long)] =
      inputStream
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Commit] {
          override def extractAscendingTimestamp(c: Commit): Long =
            c.commit.committer.date.getTime
        })
        .flatMap { commit =>
          val cleaned = commit.url.replaceAll("\\?.*$", "")
          val parts   = cleaned.split("/")
          val idxRepos = parts.indexOf("repos")
          val repo =
            if (idxRepos >= 0 && idxRepos + 2 < parts.length)
              s"${parts(idxRepos + 1)}/${parts(idxRepos + 2)}"
            else if (parts.length >= 4)
              s"${parts(parts.length - 3)}/${parts(parts.length - 2)}"
            else cleaned

          val ts = commit.commit.committer.date.getTime

          commit.files.flatMap { f =>
            for {
              name   <- f.filename
              status <- f.status
              if (status == "added" || status == "removed")
            } yield (repo, name, status, ts)
          }
        }(org.apache.flink.api.scala.createTypeInformation[(String, String, String, Long)])

    val pattern: Pattern[(String, String, String, Long), (String, String, String, Long)] =
      Pattern
        .begin[(String, String, String, Long)]("added").where(_._3 == "added")
        .followedBy("removed").where(_._3 == "removed")
        .within(Time.days(1))

    val keyed = fileEvents.keyBy(e => (e._1, e._2)) 

    val patternStream = CEP.pattern(keyed, pattern)

    patternStream.select(new PatternSelectFunction[(String, String, String, Long), (String, String)] {
      override def select(m: java.util.Map[String, java.util.List[(String, String, String, Long)]]): (String, String) = {
        val added = m.get("added").get(0)
        (added._1, added._2)
      }
    })
  }

}
