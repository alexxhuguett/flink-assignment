# Assignment 4: Flink
In this assignment you will be working with Apache Flink. Make sure you read some of their [documentation](https://flink.apache.org/) before you start.
For this assignment we are working with GitHub commits as a data source. The raw data can be found in `/data/flink_commits.json` and `/data/flink_commits_geo.json`.
The data types are defined in the `util.Protocol` class.

This assignment is quite challenging, so make sure you start on time.  

Total amount of points: **100**. 

**How to submit:**  
Make sure you do **NOT** rename the `FlinkAssignment.scala` file nor the method definitions, autograding will fail otherwise. You are not allowed to use more external libraries then given in the template.
Submit a `zip` folder of your assignment to CPM. Make sure the zip folder only contains the classes and don't use packages. For example:
```sh
- wouter_solution.zip
    FlinkAssignment.scala
    HelperClass.scala
```
Lastly, submissions to CPM for this assignment can take a while to be graded. 

## Import template
We recommend to make use of [IntelliJ IDEA](https://www.jetbrains.com/idea/) for this assignment. As a student you can get a free license. 
To import the template into IntelliJ: 1) Choose the `Import Project` option and pick the template directory, 2)  Choose `Import project from external model -> Maven`, 3) Choose your JDK. We recommend using JDK `1.8`, 4) Verify that it works by running the main method from the `FlinkAssignment.scala` class.
 
## Contents

- [Basics](#basics)
    - [Question 1](#question-1-10-points)
    - [Question 2](#question-2-10-points) 
- [State](#state) 
    - [Question 3](#question-3-10-points)
    - [Question 4](#question-4-10-points)
- [Windows](#windows) 
    - [Question 5](#question-5-10-points)
    - [Question 6](#question-6-10-points)
    - [Question 7](#question-7-10-points)
    - [Question 8](#question-8-15-points)
- [CEP](#cep)
    - [Question 9](#question-9-15-points)
    
## Basics
In this part of the assignment you are introduced to same basic functionality of Apache Flink. To get a nice introduction, you may want to read [this page](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/api_concepts.html) of the Flink documentation.
### Question 1 (10 points)

Write a Flink application which outputs the **sha** of
commits with **at least 20** additions.

Input: `DataStream[Commit]`  
Output: `DataStream[String]`  
Output format: `sha`

Example output:  
```
72e9cfa9bf75a39b22005f68f1f95c29bd82687c
b5e128c957531aa7d199ff7a9abac8d7e5c0ce7d
790293312b078063505d8137fcf342a0a34add54
```

---

### Question 2 (10 points)
Write a Flink application which outputs the **names** of the files with
**more than 30** deletions.  

Input: `DataStream[Commit]`  
Output: `DataStream[String]`  
Output format:  `fileName`

Example output:  
```
file.md
anotherFile.scala
anotherAnotherFile.py
```

## State
In this part of the assignment we will work with 'state' as explained [here](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/state.html).

### Question 3 (10 points)
**Count** the occurrences of Java and Scala files. I.e. files ending with either `.scala` or `.java`.  

Input: `DataStream[Commit]`  
Output: `DataStream[(String, Int)]`  
Output format: `(fileExtension, #occurrences)`

Example output:
```
(java, 1)
(java, 2)
(scala, 1)
(java, 3)
(scala, 2)
```
---

### Question 4 (10 points)
**Count** the total amount of **changes** for each file status (e.g. modified, removed or added) for the following extensions: `.js` and `.py`.  

Input: `DataStream[Commit]`  
Output: `DataStream[(String, String, Int)]`   
Output format:  `(extension, status, count)`

Example output:
```
(py, modified, 12)
(py, added, 6)
(js, added, 10)
(js, removed, 5)
(py, modified, 22)
```

## Windows
In this part, you need to work with Flink windows. A good starting point is [this](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/event_time.html) and [this](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/windows.html) page. As a timestamp for a commit always use the `commit.commit.committer.date` field. Make sure that Flink will use this field!    

**For this assignment, you may assume that commits are arriving in-order.**

### Question 5 (10 points)
For **every day** output the amount of commits. Include the timestamp in the following format `dd-MM-yyyy`; e.g. `(26-06-2019, 4)` meaning on the 26th of June 2019 there were 4 commits.  
Make use of a non-keyed window.   

Input: `DataStream[Commit]`  
Output: `DataStream[(String, Int)]`  
Output format: `(date, count)`

Example output:
```
(10-03-2019, 3)
(16-03-2019, 1)
(18-04-2019, 1)
```

---

### Question 6 (10 points)
Consider two types of commits: **small** commits and **large** commits whereas small: `0 <= x <= 20` and large: `x > 20` where `x = total amount of changes`.
Compute every **12 hours** the amount of small and large commits in the last **48 hours**.  

Input: `DataStream[Commit]`  
Output: `DataStream[(String, Int)]`  
Output format: `(type, count)` 

Example output:
```
(small,1)
(small,9)
(small,11)
(large,10)
(large,7)
```

--- 

### Question 7 (10 points)
For each repository compute a **daily commit summary** and output the summaries with **more than** 20 commits and **at most** 2 unique committers. The `CommitSummary` case class is already defined. The fields of this case class:

_repo_: name of the repo.  
_date_: use the start of the window in format `dd-MM-yyyy`.  
_amountOfCommits_: the number of commits on that day for that repository.  
_amountOfCommitters_: the amount of unique committers contributing to the repository.  
_totalChanges_: the sum of total changes in all commits.  
_topCommitter_: the top committer of that day i.e. with the most commits. **Note** if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`    

**Hint**: Write your own ProcessWindowFunction.

Input: `DataStream[Commit]`   
Output: `DataStream[CommitSummary]`  
Output format: `CommitSummary`

Example output:
```
CommitSummary(codefeedr/codefeedr,26-06-1997,21,4,wzorgdrager)
CommitSummary(tudelft/bigdata,30-12-2015,40,3,yoshi)
CommitSummary(tudelft/machinelearning,20-20-2019,66,6,bot)
```

---

### Question 8 (15 points)
For this exercise there is another dataset containing `CommitGeo` events. A CommitGeo event stores the _sha_ of a commit, a _date_ and the _continent_ it was produced in. 
You can assume that for every commit there is a CommitGeo event arriving within a timeframe of **1 hour before** and **30 minutes after** the commit.
Get the **weekly** amount of **changes** for the **java** files (.java extension) per **continent**.

**Hint:** Find the correct join to use!  

Input: `DataStream[Commit], DataStream[CommitGeo]`   
Output: `DataStream[(String, Int)]`  
Output format: `(continent, amount)`  

Example output:
```
(Asia,309)
(Oceania,111)
(South-America,1296)
(Africa,58)
```

## CEP
For the last question you have to make use of the CEP library of Flink. This library is documented [here](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/libs/cep.html).

### Question 9 (15 points)
Find all **files** that were added and removed within **one day**.

**Hint:** Use the Complex Event Processing library (CEP).  

Input: `DataStream[Commit]`   
Output: `DataStream[(String, String)]`  
Output format: `(repository, filename)`

Example output:
```
(codefeedr/codefeedr, AwesomeButNotSoAwesomeClass.scala)
(tudelft/bigdata, ThisFileWillBeRemovedWithinOne.day)
(wouter/coolproject, oops.md)
```

