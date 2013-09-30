# Ambrose [![Build Status](https://secure.travis-ci.org/twitter/ambrose.png)](http://travis-ci.org/twitter/ambrose)

更改后的ambrose

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

Next, you can try running the Ambrose demo on your local machine. The demo starts a local web server
which serves the front-end client resources and sample data. Start the demo with the following
command and then browse to
[http://localhost:8080/workflow.html?localdata=large](http://localhost:8080/workflow.html?localdata=large):

```
./bin/ambrose-demo
```

To run Ambrose with an actual Hive script, you'll need to build the Ambrose Hive distribution:

```
mvn package
```

You can then run the following commands to execute `path/to/my/script.sql` with an Ambrose app server
embedded within the Hive client:

```
cd pig/target/ambrose-hive-$VERSION-bin/ambrose-hive-$VERSION
./bin/ambrose-hive -f path/to/my/script.sql
```

Note that this command delegates to the `hive` script present in your local installation of Hive, so
make sure `$HIVE_HOME/bin` is in your path. Now, browse to
[http://localhost:8080/dashboard.html](http://localhost:8080/dashboard.html) to see the
progress of your script using the Ambrose UI. To override the default port, export `AMBROSE_PORT`
before invoking `pig-ambrose`:

```
export AMBROSE_PORT=4567
```

## Maven repository

Ambrose releases can be found on Maven under [com.twitter.ambrose](http://repo1.maven.org/maven2/com/twitter/ambrose).

## How to contribute

Bug fixes, features, and documentation improvements are welcome! Please fork the project and send us
a pull request on Github. You can [submit issues on Github](https://github.com/twitter/ambrose/issues)
as well.

Here are some high-level goals we'd love to see contributions for:

* Improve the front-end client
* Add other visualization options
* Create a new back-end for a different runtime environment
* Create a standalone Ambrose server that's not embedded in the workflow client

## Versioning

For transparency and insight into our release cycle, releases will be numbered with the follow format:

`<major>.<minor>.<patch>`

And constructed with the following guidelines:

* Breaking backwards compatibility bumps the major
* New additions without breaking backwards compatibility bumps the minor
* Bug fixes and misc changes bump the patch

For more information on semantic versioning, please visit http://semver.org/.

## Authors

* Bill Graham ([@billgraham](https://twitter.com/billgraham))
* Andy Schlaikjer ([@sagemintblue](https://twitter.com/sagemintblue))
* Gary Helmling ([@gario](https://twitter.com/gario))
* Nicolas Belmonte ([@philogb](https://twitter.com/philogb))

## License

Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
