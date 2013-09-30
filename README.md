# Ambrose [![Build Status](https://secure.travis-ci.org/twitter/ambrose.png)](http://travis-ci.org/twitter/ambrose)

Ambrose provides the following in a web UI:

* A table view of all the associated jobs, along with their current state
* Chord and graph diagrams to visualize job dependencies and current state
* An overall script progress bar

Ambrose is built using the following front-end technologies:

* [D3.js](http://d3js.org) - For diagram generation
* [Bootstrap](http://twitter.github.com/bootstrap/) - For layout and CSS support

Ambrose is designed to support any workflow runtime, but current support is limited to [Apache
Hive](http://hive.apache.org/).

Follow [@Ambrose](https://twitter.com/ambrose) on Twitter to stay in touch!

## Supported runtimes

* [Hive](http://hive.apache.org/) See [hive/README.md](https://github.com/twitter/ambrose/blob/master/hive/README.md)

## Examples

Below is a screenshot of the Ambrose UI. The interface presents multiple responsive "views" of a
single workflow. Just beneath the toolbar at the top of the screen is a workflow progress bar that
tracks overall completion percentage of the workflow. Below the progress bar are two diagrams which
depict the workflow's jobs and their dependencies. Below the diagrams is a table of workflow
jobs.

All views react to mouseover and click events on a job, regardless of the view on which the event is
triggered; Moving your mouse over the first row of the table will highlight that job row along with
the associated job arc in the chord diagram and job node in the graph diagram. Clicking on a job in
any view will select it, updating the highlighting of that job in all views. Clicking twice on the
same job will deselect it.

![Ambrose UI screenshot](https://github.com/twitter/ambrose/raw/master/docs/img/ambrose-ss1.png)

## Quickstart

To get started with Ambrose, first clone the Ambrose Github repository:

```
git clone https://github.com/jy01649210/ambrose.git
cd ambrose
```

To run Ambrose with an actual Hive script, you'll need to build the Ambrose Hive distribution:

```
mvn package
```

##Building with 3 projects:
##1.common: general purpose module and utilities
##2.hive: hook jar to get hive runtime information
##3.web: hive runtime information representation

Install redis and start it

Install jetty and start it

You can then run the following commands to execute `path/to/my/script.sql` with an Ambrose app server
embedded within the Hive client:

```
cd hive/target/ambrose-hive-$VERSION-bin/ambrose-hive-$VERSION
./bin/ambrose-hive -f path/to/my/script.sql
```

Note that this command delegates to the `hive` script present in your local installation of Hive, so
make sure `$HIVE_HOME/bin` is in your path. 

Then, deploy the .war file in web folder to your jetty webapps/ROOT, browse to
[http://localhost:8080/dashboard.html](http://localhost:8080/dashboard.html) to see the
progress of your script using the Ambrose UI. 

Here are some high-level goals we'd love to see contributions for:

* Improve the front-end client
* Add other visualization options
