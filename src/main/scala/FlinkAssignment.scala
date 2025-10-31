import java.text.SimpleDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.Protocol.{Commit, CommitGeo, CommitSummary}
import util.{CommitGeoParser, CommitParser}

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
      // use the commit's committer date as event time (arrives in order per assignment)
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Commit] {
        override def extractAscendingTimestamp(c: Commit): Long =
          c.commit.committer.date.getTime
      })
      // non-keyed daily tumbling window
      .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
      // emit (dd-MM-yyyy, count)
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
      .filter{commit =>
        val commitDate = commit.commit.committer.date
        val now = new Date()
        val diffHours = (now.getTime - commitDate.getTime) / (1000 * 60 * 60)
        diffHours <= 48
      }
      .map{commit =>
        val total = commit.stats.map(stats => stats.total).getOrElse(0)
        val commitType = if (total > 20) "large" else "small"
        (commitType, 1)
      }
      .keyBy(_._1)
      .timeWindow(Time.hours(12))
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

    def repoFromUrl(url: String): String = {
      val marker = "/repos/"
      val i = url.indexOf(marker)
      if (i >= 0) {
        val rest = url.substring(i + marker.length)
        val parts = rest.split("/", 3)
        if (parts.length >= 2) s"${parts(0)}/${parts(1)}" else rest
      } else url
    }

    def committerId(c: Commit): String =
      c.committer.map(_.login).getOrElse(c.commit.committer.name)

    commitStream
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Commit] {
        override def extractAscendingTimestamp(c: Commit): Long =
          c.commit.committer.date.getTime
      })
      .keyBy(c => repoFromUrl(c.url))
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .apply { (repo: String, window: TimeWindow, elems: Iterable[Commit], out: Collector[CommitSummary]) =>
        val fmt = new SimpleDateFormat("dd-MM-yyyy")
        val date = fmt.format(new Date(window.getStart))

        var commits = 0
        var totalChanges = 0
        val byCommitter = scala.collection.mutable.Map.empty[String, Int].withDefaultValue(0)

        elems.foreach { c =>
          commits += 1
          totalChanges += c.stats.map(_.total).getOrElse(0)
          val id = committerId(c)
          byCommitter(id) = byCommitter(id) + 1
        }

        val uniqueCommitters = byCommitter.size
        val maxByOne = if (byCommitter.isEmpty) 0 else byCommitter.values.max
        val topCommitter =
          byCommitter.collect { case (k, v) if v == maxByOne => k }.toSeq.sorted.mkString(",")

        if (commits > 20 && uniqueCommitters <= 2) {
          out.collect(
            CommitSummary(
              repo = repo,
              date = date,
              amountOfCommits = commits,
              amountOfCommitters = uniqueCommitters,
              totalChanges = totalChanges,
              mostPopularCommitter = topCommitter
            )
          )
        }
      }
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
      geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = {
    val commit = commitStream
      .flatMap(commit => commit.files.map(file => (commit.sha, file)))
      .filter{ case (s, f) => f.filename.exists(name => name.endsWith(".java"))} //sha, file
      .map { case (sha, file) => (sha, file.changes) }
      .keyBy(_._1)
      .sum(1) // sha, changes per sha

    geoStream
      .keyBy(_.sha)
      .intervalJoin(commit.keyBy(_._1)) // geostream, sha, changes
      .between(Time.hours(-1), Time.minutes(30))
      .process(new ProcessJoinFunction[CommitGeo, (String, Int), (String, Int)] {
        override def processElement(
                                     geo: CommitGeo,
                                     commit: (String, Int),
                                     ctx: ProcessJoinFunction[CommitGeo, (String, Int), (String, Int)]#Context,
                                     out: Collector[(String, Int)]
                                   ): Unit = {
          out.collect((geo.continent, commit._2))
        }
      })
      .keyBy(_._1)
      .timeWindow(Time.days(7))
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

    case class FileEvent(repo: String, filename: String, status: String, ts: Long)

    def repoFromUrl(url: String): String = {
      val i = url.indexOf("/repos/")
      val j = url.indexOf("/commits/", math.max(i, 0))
      if (i >= 0 && j > i + 7) url.substring(i + 7, j) else "unknown/unknown"
    }

    val fileEvents: DataStream[FileEvent] =
      inputStream
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Commit]() {
          override def extractAscendingTimestamp(c: Commit): Long =
            c.commit.committer.date.getTime
        })
        .flatMap { c =>
          val repo = repoFromUrl(c.url)
          val ts   = c.commit.committer.date.getTime
          c.files.flatMap { f =>
            for {
              name   <- f.filename
              status <- f.status
            } yield FileEvent(repo, name, status, ts)
          }
        }

    // Key by (repo, filename)
    val keyed = fileEvents.keyBy(fe => (fe.repo, fe.filename))

    // Pattern: added -> removed within 1 day
    val pattern = Pattern
      .begin[FileEvent]("added").where(_.status == "added")
      .next("removed").where(_.status == "removed")
      .within(Time.days(1))

    val patternStream = CEP.pattern(keyed, pattern)

    patternStream.select((pattern: scala.collection.Map[String, Iterable[FileEvent]]) => {
      val added = pattern("added").head
      val removed = pattern("removed").head
      (added.repo, added.filename)
    })
  }

}
